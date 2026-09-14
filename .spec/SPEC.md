# SPEC — STP (Sistema de Transferência de Porquinhos)

## 1. OBJETIVO
O App do Inter transfere Piggies entre contas de países distintos, com débito e crédito autorizados e confirmados de ponta a ponta, sem perder nem duplicar saldo.

## 2. CONTRATOS

**Borda HTTP (assíncrona: responde 202 e processa via Kafka)**
```
POST /transfers                 Req: {fromAccount, fromCountry, toAccount, toCountry, amountMinor, idempotencyKey}
                                202 {transferId, status:PENDING}  | 400 problem+json
GET  /transfers/{transferId}    200 {transferId, status, step, failureReason?, updatedAt}
```
`status: PENDING | DEBIT_AUTHORIZED | CREDIT_AUTHORIZED | COMPLETED | FAILED | COMPENSATED`

**Kafka (key = transferId; at-least-once; 1 instância de consumer por país)**
```
piggies.withdraw.cmd.<country>   AuthorizeDebit{transferId, account, amountMinor} | ConfirmDebit{transferId} | CancelDebit{transferId}
piggies.withdraw.evt             DebitAuthorized | DebitRejected{reason} | DebitConfirmed | DebitCancelled   (+transferId)
piggies.deposit.cmd.<country>    AuthorizeCredit{transferId, account, amountMinor} | ConfirmCredit{transferId}
piggies.deposit.evt              CreditAuthorized | CreditRejected{reason} | CreditConfirmed                 (+transferId)
```

**Entidades**
```
Coordinator: Transfer(id, fromAccount, fromCountry, toAccount, toCountry, amountMinor, status, step, failureReason, idempotencyKey UNIQUE, createdAt, updatedAt)
Withdraw   : Account(id, country, balanceMinor, heldMinor)  Hold(id, transferId UNIQUE, accountId, amountMinor, state: AUTHORIZED|CONFIRMED|CANCELLED)
Deposit    : Account(id, country, balanceMinor)             PendingCredit(id, transferId UNIQUE, accountId, amountMinor, state: AUTHORIZED|CONFIRMED)
```
Dinheiro sempre `long amountMinor` (centésimos de Piggie). Nunca `double`.

## 3. CRITÉRIOS DE ACEITE
- [ ] `POST /transfers` válido → 202 com `transferId`, `status=PENDING`, em < 100ms (sem chamada síncrona a Withdraw/Deposit).
- [ ] Transferência de 100 com saldo 500 (BRA→USA) → em até 5s `GET /transfers/{id}` = `COMPLETED`; origem 400, destino +100.
- [ ] Saldo insuficiente (transfere 600 de 500) → `FAILED` com `failureReason=INSUFFICIENT_FUNDS`; nenhum saldo alterado; nenhum comando enviado ao Deposit.
- [ ] Crédito rejeitado (conta destino inexistente) → `CancelDebit` emitido; `COMPENSATED`; saldo de origem volta ao valor inicial e `heldMinor=0`.
- [ ] Reentrega duplicada de `AuthorizeDebit` (mesmo transferId) → um único `Hold`; saldo debitado uma só vez.
- [ ] Dois `POST /transfers` com o mesmo `idempotencyKey` → mesmo `transferId`, uma única transferência executada.
- [ ] Withdraw-BRA e Deposit-USA usam datasources distintos: derrubar o banco de USA não afeta leitura/escrita em BRA.
- [ ] Autorização sem resposta em 5s → `FAILED` com `failureReason=TIMEOUT` + compensação do que já foi autorizado.
- [ ] Testes: unitários da máquina de estados da saga e das regras de saldo/hold; integração com Testcontainers (Postgres + Kafka) cobrindo os 3 primeiros critérios.

## 4. FATIAS (contrato congelado primeiro → 3 devs em paralelo, sem bloqueio)
**Minuto 0, juntos (15 min):** records de comando/evento, nomes de tópicos e `amountMinor` num pacote `contracts` compartilhado. Congelado isso, ninguém espera ninguém — cada fatia sobe e se demonstra sozinha contra o Kafka.
1. **Dev A — Coordinator: borda async + saga.** `POST /transfers` (202), persistência do `Transfer`, máquina de estados e timeout. *Demo sem B e C:* POST → 202; `kafka-console-consumer` mostra `AuthorizeDebit`; injetar `DebitAuthorized`/`CreditAuthorized`/`DebitRejected` por `kafka-console-producer` e ver `GET /transfers/{id}` caminhar até `COMPLETED`/`FAILED`/`COMPENSATED`.
2. **Dev B — Withdraw (BRA): hold, confirm, cancel.** Reserva sobre `balanceMinor`/`heldMinor`, idempotência por `transferId`. *Demo sem A e C:* produzir `AuthorizeDebit` no tópico → sai `DebitAuthorized` e o hold aparece no Postgres de BRA; reentregar a mesma mensagem → um único `Hold`; saldo insuficiente → `DebitRejected`.
3. **Dev C — Deposit (USA): pending credit, confirm.** Mesmo desenho do crédito, datasource e consumer group próprios. *Demo sem A e B:* produzir `AuthorizeCredit` → `CreditAuthorized`; conta inexistente → `CreditRejected`; `ConfirmCredit` duplicado → saldo creditado uma só vez.

**Integração (últimos 10 min, quem terminar primeiro):** os 3 serviços + Postgres/Kafka via Testcontainers; happy path BRA→USA e o cenário de compensação viram um teste de integração único.

## 5. RISCOS / AMBIGUIDADES (decisão default)
- **Sem 2PC.** Adoto saga orquestrada com compensação (`CancelDebit`); a janela entre débito confirmado e crédito confirmado é resolvida por retry idempotente, não por rollback.
- **Câmbio BRA↔USA não especificado.** Assumo paridade 1:1 e mesmo `amountMinor` nas duas pontas; FX fica fora do escopo.
- **"Instância isolada por país".** Interpreto como um processo + um banco por (serviço, país). No live code: mesmo artefato Maven, ambientes `coordinator | withdraw-bra | deposit-usa`, cada um com seu datasource e seu grupo de consumo — fronteira por configuração, não por repositório.
- **Origem dos saldos.** Não há cadastro no enunciado e não crio endpoint para isso: contas são pré-existentes, carregadas por `import.sql` em cada instância e por fixture nos testes. Onboarding de conta está fora do STP.
- **Trabalho paralelo depende do contrato.** Mudar um record de mensagem no meio quebra as três fatias de uma vez; o pacote `contracts` só muda por acordo explícito, e campo novo entra como opcional (nunca renomear/remover durante a hora).
- **Confirmação parcial.** Se `ConfirmCredit` falhar após `ConfirmDebit`, não desfaço: retry infinito com backoff e alerta (dinheiro já saiu); reverter criaria crédito fantasma.
- **Entrega at-least-once do Kafka.** Toda escrita é idempotente por `transferId`; sem transações Kafka (exactly-once) para caber em 1h.
- **Schema.** Mantenho `hbm2ddl=update` do skeleton; Flyway seria o certo em produção.
