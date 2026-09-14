# SPEC — STP (Sistema de Transferência de Porquinhos)

## 1. OBJETIVO
O App do Inter transfere Piggies entre contas de países distintos, com débito e crédito autorizados e confirmados de ponta a ponta, sem perder nem duplicar saldo.

## 2. CONTRATOS

**Borda HTTP (assíncrona: responde 202 e processa via Kafka)**
```
POST /transfers                 Req: {fromAccount, fromCountry, toAccount, toCountry, amountMinor, idempotencyKey}
                                202 {transferId, status:PENDING}  | 400 problem+json
GET  /transfers/{transferId}    200 {transferId, status, step, failureReason?, updatedAt}
POST /accounts (demo/seed)      201 {accountId, country, balanceMinor}
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

## 4. FATIAS (cada uma atravessa os 3 serviços, demonstrável isolada)
1. **Happy path multi-país** — `POST /transfers` → 202; Coordinator publica `AuthorizeDebit` (BRA) → `AuthorizeCredit` (USA) → `Confirm*`; `GET /transfers/{id}=COMPLETED`, saldos corretos nos dois bancos. *Demo:* 1 POST + 1 GET + `select` nos dois Postgres.
2. **Rejeição e compensação** — `DebitRejected` por saldo insuficiente → `FAILED` sem tocar no Deposit; `CreditRejected` após débito autorizado → `CancelDebit` → `COMPENSATED`. *Demo:* dois cenários de erro, saldos inalterados ao final.
3. **Idempotência, retry e timeout** — chave única por `transferId` em Hold/PendingCredit, `idempotencyKey` no Coordinator, timeout de autorização com compensação. *Demo:* replay manual da mesma mensagem Kafka + POST duplicado; saldo e estado estáveis.

## 5. RISCOS / AMBIGUIDADES (decisão default)
- **Sem 2PC.** Adoto saga orquestrada com compensação (`CancelDebit`); a janela entre débito confirmado e crédito confirmado é resolvida por retry idempotente, não por rollback.
- **Câmbio BRA↔USA não especificado.** Assumo paridade 1:1 e mesmo `amountMinor` nas duas pontas; FX fica fora do escopo.
- **"Instância isolada por país".** Interpreto como um processo + um banco por (serviço, país). No live code: mesmo artefato Maven, ambientes `coordinator | withdraw-bra | deposit-usa`, cada um com seu datasource e seu grupo de consumo — fronteira por configuração, não por repositório.
- **Origem dos saldos.** Não há cadastro no enunciado; exponho `POST /accounts` apenas para seed/demo, marcado como não-produtivo.
- **Confirmação parcial.** Se `ConfirmCredit` falhar após `ConfirmDebit`, não desfaço: retry infinito com backoff e alerta (dinheiro já saiu); reverter criaria crédito fantasma.
- **Entrega at-least-once do Kafka.** Toda escrita é idempotente por `transferId`; sem transações Kafka (exactly-once) para caber em 1h.
- **Schema.** Mantenho `hbm2ddl=update` do skeleton; Flyway seria o certo em produção.
