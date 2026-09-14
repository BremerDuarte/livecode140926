# Fatia 2 — Withdraw (BRA): hold, confirm, cancel

Plano de implementação da fatia 2 de `.spec/SPEC.md` (Dev B).
Stack: Java 25 + Micronaut 5.1.3 (Netty) + Hibernate/JPA + PostgreSQL + Kafka + Maven + Lombok.

Branch sugerida: `feature/withdraw-service` (a fatia 1 vive em `feature/coordinator-service`).

---

## 1. Escopo

**Entra:**
- Consumir `piggies.withdraw.cmd.BRA`: `AuthorizeDebit`, `ConfirmDebit`, `CancelDebit`.
- Produzir em `piggies.withdraw.evt`: `DebitAuthorized`, `DebitRejected`, `DebitConfirmed`, `DebitCancelled`.
- Entidades `Account(id, country, balanceMinor, heldMinor)` e `Hold(id, transferId UNIQUE, accountId, amountMinor, state)`.
- Idempotência por `transferId` (entrega at-least-once do Kafka).
- Datasource e consumer group próprios do país, via configuração (`withdraw-bra`).
- Testes unitários das regras de saldo/hold + um teste de integração que prova a demo da fatia.

**Não entra (é de outra fatia ou está fora do STP):**
- Nenhum endpoint HTTP. Withdraw só fala Kafka.
- Nada de saga, `Transfer`, timeout ou `GET /transfers` (fatia 1).
- Nada de crédito/`Deposit` (fatia 3) — inclusive nenhuma classe compartilhada com ele.
- Cadastro de conta: saldos vêm de `import.sql` e de fixtures (decisão do SPEC §5).
- Câmbio: paridade 1:1, `amountMinor` chega pronto.

**Critérios de aceite do SPEC cobertos por esta fatia:** saldo insuficiente → `DebitRejected(INSUFFICIENT_FUNDS)` sem alterar saldo; reentrega duplicada de `AuthorizeDebit` → um único `Hold`; datasource isolado por país; e o lado do débito dos cenários de happy path e de compensação.

---

## 2. Pré-requisito: pacote `contracts` (congelado, propriedade das 3 fatias)

Se ainda não existir, o Dev B cria — mas **só o lado withdraw**, e nunca renomeia/remove campo depois (SPEC §5).

```
src/main/java/co/inter/piggies/contracts/
├── Topics.java
├── withdraw/WithdrawCommand.java   (sealed: AuthorizeDebit | ConfirmDebit | CancelDebit)
├── withdraw/AuthorizeDebit.java    record(String transferId, String account, long amountMinor)
├── withdraw/ConfirmDebit.java      record(String transferId)
├── withdraw/CancelDebit.java       record(String transferId)
├── withdraw/WithdrawEvent.java     (sealed: DebitAuthorized | DebitRejected | DebitConfirmed | DebitCancelled)
├── withdraw/DebitAuthorized.java   record(String transferId)
├── withdraw/DebitRejected.java     record(String transferId, String reason)
├── withdraw/DebitConfirmed.java    record(String transferId)
└── withdraw/DebitCancelled.java    record(String transferId)
```

```java
public final class Topics {
    public static final String WITHDRAW_CMD_PREFIX = "piggies.withdraw.cmd.";
    public static final String WITHDRAW_EVT = "piggies.withdraw.evt";
    private Topics() {}
}
```

**Discriminador de tipo.** Os três comandos dividem um tópico, então o JSON carrega um campo `type`:

```java
@Serdeable
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = AuthorizeDebit.class, name = "AuthorizeDebit"),
    @JsonSubTypes.Type(value = ConfirmDebit.class,   name = "ConfirmDebit"),
    @JsonSubTypes.Type(value = CancelDebit.class,    name = "CancelDebit")})
public sealed interface WithdrawCommand permits AuthorizeDebit, ConfirmDebit, CancelDebit {
    String transferId();
}
```

Idem para `WithdrawEvent`. Cada record leva `@Serdeable`. Sealed interface + `switch` com pattern matching dá exaustividade em tempo de compilação e o `type` no JSON deixa a demo com `kafka-console-producer` trivial.

> Plano B se o `micronaut-serde` reclamar de polimorfismo: consumir o payload como `String` e desserializar à mão com um `switch` sobre o campo `type`. Custa 10 linhas no listener e não vaza para o domínio. Decida isso nos 5 primeiros minutos, não no meio da fatia.

`reason` é `String` (não enum) de propósito: um valor novo não quebra a desserialização do coordinator, que só copia para `failureReason`. Valores desta fatia: `INSUFFICIENT_FUNDS`, `ACCOUNT_NOT_FOUND`.

---

## 3. Estrutura da feature (feature-sliced + camadas)

```
src/main/java/co/inter/piggies/withdraw/
├── domain/
│   ├── Account.java              @Entity + regras de saldo
│   ├── Hold.java                 @Entity + transições de estado
│   ├── HoldState.java            enum AUTHORIZED | CONFIRMED | CANCELLED
│   └── RejectionReason.java      constantes INSUFFICIENT_FUNDS / ACCOUNT_NOT_FOUND
├── application/
│   └── WithdrawService.java      3 casos de uso @Transactional; devolve o evento
└── infrastructure/
    ├── persistence/AccountRepository.java
    ├── persistence/HoldRepository.java
    └── messaging/WithdrawCommandListener.java   @KafkaListener + @KafkaClient do publisher
```

Dependências apontam só para dentro: `infrastructure → application → domain`. `domain` não importa Micronaut, Kafka nem `contracts`.

---

## 4. Camada `domain`

### `Account`

```java
@Entity @Table(name = "withdraw_account")
@Getter @NoArgsConstructor @AllArgsConstructor
public class Account {
    @Id private String id;          // número da conta; é o que vem no comando
    private String country;
    private long balanceMinor;      // saldo total, inclui o que está retido
    private long heldMinor;         // parcela retida por holds AUTHORIZED
}
```

Regras (métodos com comportamento, é o que os testes unitários exercitam):

| Método | Regra |
|---|---|
| `availableMinor()` | `balanceMinor - heldMinor` |
| `canHold(long amount)` | `amount > 0 && amount <= availableMinor()` |
| `hold(long amount)` | `heldMinor += amount`; `IllegalStateException` se `!canHold` |
| `settle(long amount)` | `balanceMinor -= amount; heldMinor -= amount` (confirmação) |
| `release(long amount)` | `heldMinor -= amount` (cancelamento; saldo volta ao original) |

Semântica alinhada aos critérios: autorizar 100 sobre 500 deixa `balance=500, held=100`; confirmar deixa `400/0`; cancelar volta para `500/0`.

Dinheiro é sempre `long` em centésimos. Nenhum `double`, `float` ou `BigDecimal` nesta fatia.

### `Hold`

```java
@Entity @Table(name = "withdraw_hold",
       uniqueConstraints = @UniqueConstraint(columnNames = "transfer_id"))
```
Campos: `id` (identity), `transferId` (UNIQUE), `accountId` (nullable — ver tombstone), `amountMinor`, `state`.

O `Hold` **não é só uma reserva: é o registro da decisão tomada para aquele `transferId`.** É isso que dá idempotência de graça e resolve o cancelamento fora de ordem.

Transições: `AUTHORIZED → CONFIRMED`, `AUTHORIZED → CANCELLED`. `CONFIRMED` é terminal (dinheiro já saiu; SPEC §5 "confirmação parcial"). `CANCELLED` é terminal.

---

## 5. Camada `application` — `WithdrawService`

Três métodos `@Transactional`, cada um recebe o comando e **devolve o evento** (`Optional<WithdrawEvent>`):

```java
Optional<WithdrawEvent> authorize(AuthorizeDebit cmd);
Optional<WithdrawEvent> confirm(ConfirmDebit cmd);
Optional<WithdrawEvent> cancel(CancelDebit cmd);
```

> **Decisão-chave:** o service não publica em Kafka, só decide. Quem publica é o listener, **depois** do commit. Isso (a) mantém o Kafka fora da transação do banco, (b) evita anunciar um débito que deu rollback, (c) torna o service testável em JUnit puro, sem broker. Se a publicação falhar depois do commit, a reentrega do comando reexecuta o caminho idempotente e reemite o mesmo evento.

### `authorize` — tabela de decisão

| Estado | Efeito | Evento |
|---|---|---|
| sem `Hold`, conta existe no país, `canHold` | cria `Hold(AUTHORIZED)`, `account.hold(v)` | `DebitAuthorized` |
| sem `Hold`, conta inexistente ou de outro país | nenhum | `DebitRejected(ACCOUNT_NOT_FOUND)` |
| sem `Hold`, `available < amount` | nenhum | `DebitRejected(INSUFFICIENT_FUNDS)` |
| `Hold` já `AUTHORIZED` | nenhum | `DebitAuthorized` (reemitido) |
| `Hold` já `CONFIRMED` | nenhum | `DebitConfirmed` |
| `Hold` já `CANCELLED` | nenhum | `DebitCancelled` |

Reemitir o evento do estado atual (em vez de ignorar) é o que impede o coordinator de ficar pendurado quando a resposta original se perdeu.

### `confirm`

| Estado | Efeito | Evento |
|---|---|---|
| `AUTHORIZED` | `account.settle(v)`, `hold.confirm()` | `DebitConfirmed` |
| `CONFIRMED` | nenhum | `DebitConfirmed` (reemitido) |
| `CANCELLED` | nenhum, log ERROR | nenhum |
| não existe | nenhum, log WARN | nenhum |

### `cancel`

| Estado | Efeito | Evento |
|---|---|---|
| `AUTHORIZED` | `account.release(v)`, `hold.cancel()` | `DebitCancelled` |
| `CANCELLED` | nenhum | `DebitCancelled` (reemitido) |
| `CONFIRMED` | nenhum, log ERROR (dinheiro já saiu) | nenhum |
| não existe | grava **tombstone** `Hold(transferId, accountId=null, amountMinor=0, CANCELLED)` | `DebitCancelled` |

**Por que o tombstone.** Critério 8 do SPEC: no timeout o coordinator manda `CancelDebit` para um débito que talvez ainda esteja em voo. Sem o tombstone, o `AuthorizeDebit` atrasado criaria um hold que ninguém mais confirma nem cancela — saldo retido para sempre. Com ele, o `authorize` posterior encontra `CANCELLED` e não reserva nada. São três linhas e fecham o vazamento.

### Concorrência

`AccountRepository.findForUpdate(...)` com `@Lock(LockModeType.PESSIMISTIC_WRITE)` (`SELECT ... FOR UPDATE`). Há uma instância de consumer por país e a chave é o `transferId`, então contenção é rara — mas o lock garante a invariante sob reentrega concorrente sem precisar de loop de retry (mais simples que `@Version`).

Rede de segurança: capturar violação da unique de `transfer_id` (duas reentregas simultâneas) e tratar como duplicata → reemite `DebitAuthorized`.

---

## 6. Camada `infrastructure`

### Repositórios

Interfaces **estreitas**, estendendo `GenericRepository` (marcador sem métodos), declarando só o necessário:

```java
@Repository
public interface AccountRepository extends GenericRepository<Account, String> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Account> findByIdAndCountry(String id, String country);
    Account update(Account account);
}

@Repository
public interface HoldRepository extends GenericRepository<Hold, Long> {
    Optional<Hold> findByTransferId(String transferId);
    Hold save(Hold hold);
    Hold update(Hold hold);
}
```

Não estender `CrudRepository`: além de não precisar de 20 métodos, a interface enxuta permite escrever **fakes em memória de ~10 linhas** nos testes unitários (o pom não tem Mockito — e não precisa ter).

### Listener + publisher

```java
@Requires(property = "withdraw.country")
@KafkaListener(groupId = "${withdraw.consumer-group}",
               offsetReset = OffsetReset.EARLIEST,
               offsetStrategy = OffsetStrategy.SYNC_PER_RECORD,
               errorStrategy = @ErrorStrategy(value = ErrorStrategyValue.RETRY_ON_ERROR,
                                              retryCount = 3, retryDelay = "1s"))
public class WithdrawCommandListener {

    @Topic("${withdraw.commands-topic}")
    void onCommand(@KafkaKey String transferId, WithdrawCommand command) {
        Optional<WithdrawEvent> event = switch (command) {
            case AuthorizeDebit c -> service.authorize(c);
            case ConfirmDebit   c -> service.confirm(c);
            case CancelDebit    c -> service.cancel(c);
        };
        event.ifPresent(e -> publisher.publish(transferId, e));   // fora da transação
    }
}

@Requires(property = "withdraw.country")
@KafkaClient
public interface WithdrawEventPublisher {
    @Topic(Topics.WITHDRAW_EVT)
    void publish(@KafkaKey String transferId, WithdrawEvent event);
}
```

`@Requires(property = "withdraw.country")` é o que impede o consumer de subir no ambiente do coordinator ou do deposit — fronteira por configuração, como o SPEC decidiu (§5).

`SYNC_PER_RECORD` + `RETRY_ON_ERROR`: commit do offset só depois do processamento, e falha transitória é reentregue. Como toda escrita é idempotente por `transferId`, reprocessar é seguro.

---

## 7. Configuração e seed

`src/main/resources/application-withdraw-bra.yml`:

```yaml
withdraw:
  country: BRA
  commands-topic: piggies.withdraw.cmd.BRA
  consumer-group: withdraw-bra
datasources:
  default:
    url: jdbc:postgresql://localhost:5433/withdraw_bra   # instância própria de BRA
    username: piggies
    password: piggies
jpa:
  default:
    properties:
      hibernate:
        hbm2ddl:
          auto: update
          import_files: /withdraw-bra-import.sql
kafka:
  bootstrap:
    servers: localhost:9092
```

`src/main/resources/withdraw-bra-import.sql` — contas pré-existentes (SPEC §5):

```sql
insert into withdraw_account (id, country, balance_minor, held_minor)
values ('BRA-1', 'BRA', 500, 0) on conflict (id) do nothing;
```

Sobe com `-Dmicronaut.environments=withdraw-bra`. O `import_files` por ambiente é o que mantém BRA e USA com seeds distintos no mesmo artefato Maven.

---

## 8. Testes unitários (JUnit 5 + AssertJ, sem contexto Micronaut, sem Docker)

`src/test/java/co/inter/piggies/withdraw/`

### `domain/AccountTest`
- `availableMinor` desconta o retido
- `hold` de 100 sobre 500 → `balance=500, held=100`
- `hold` do valor exatamente disponível é aceito (limite)
- `hold` acima do disponível é recusado (`canHold` false)
- `hold` de valor zero/negativo é recusado
- `settle(100)` → `400/0`
- `release(100)` → `500/0`
- `hold` + `hold` do mesmo valor esgota o disponível e o terceiro é recusado

### `domain/HoldTest`
- `AUTHORIZED → confirm()` → `CONFIRMED`
- `AUTHORIZED → cancel()` → `CANCELLED`
- `confirm()` sobre `CANCELLED` lança
- `cancel()` sobre `CONFIRMED` lança

### `application/WithdrawServiceTest` (fakes em memória para os dois repositórios)
| Cenário | Esperado |
|---|---|
| authorize 100 / saldo 500 | `DebitAuthorized`; 1 hold `AUTHORIZED`; `500/100` |
| authorize 600 / saldo 500 | `DebitRejected(INSUFFICIENT_FUNDS)`; nenhum hold; `500/0` |
| authorize conta inexistente | `DebitRejected(ACCOUNT_NOT_FOUND)`; nada persistido |
| authorize conta de outro país | `DebitRejected(ACCOUNT_NOT_FOUND)` (isolamento) |
| **authorize duplicado (mesmo transferId)** | `DebitAuthorized` reemitido; **um único hold**; `500/100` |
| confirm após authorize | `DebitConfirmed`; `400/0`; hold `CONFIRMED` |
| confirm duplicado | `DebitConfirmed` reemitido; ainda `400/0` |
| cancel após authorize | `DebitCancelled`; `500/0`; hold `CANCELLED` |
| cancel duplicado | `DebitCancelled` reemitido; ainda `500/0` |
| cancel depois de confirm | nenhum evento; `400/0` inalterado |
| **cancel antes do authorize** | `DebitCancelled` + tombstone; o authorize seguinte devolve `DebitCancelled` e **não retém saldo** |
| authorize sobre hold `CONFIRMED` | `DebitConfirmed` |

### `infrastructure/WithdrawCommandListenerTest`
Service fake: cada tipo de comando cai no método certo e o evento devolvido é publicado com o `transferId` como chave; `Optional.empty()` não publica nada.

---

## 9. Teste de integração da fatia (a demo, automatizada)

`WithdrawFlowIT extends AbstractContainerTest` — reaproveita `co.inter.piggies.support.TestContainers` (Postgres 16 + Kafka já pinados no skeleton), com `TestContainers.createTopic(...)` para os dois tópicos e as propriedades `withdraw.*` injetadas via `getProperties()`. Um `@KafkaClient` de teste produz comandos e um `@KafkaListener` de teste coleta eventos; asserções com Awaitility.

1. produz `AuthorizeDebit` → chega `DebitAuthorized` e o hold aparece no Postgres
2. reproduz **a mesma mensagem** → continua um único hold, `heldMinor` inalterado
3. `AuthorizeDebit` de 600 sobre 500 → `DebitRejected(INSUFFICIENT_FUNDS)`, saldos intactos
4. `AuthorizeDebit` → `ConfirmDebit` → `DebitConfirmed`, `400/0`
5. `AuthorizeDebit` → `CancelDebit` → `DebitCancelled`, `500/0`

> A entidade `support/Piggy` do skeleton continua onde está: `PostgresContainerTest` ainda assere a tabela `piggy`. Removê-la é tarefa da integração final, não desta fatia.

---

## 10. Ordem de execução (~45 min, commits pequenos)

| # | Passo | Tempo | Prova de que funciona |
|---|---|---|---|
| 1 | `contracts/withdraw` + `Topics` (ou consumir o que a fatia 1 já congelou) | 5 min | compila |
| 2 | `domain`: `Account`, `Hold`, `HoldState`, `RejectionReason` + `AccountTest`, `HoldTest` | 10 min | testes de domínio verdes, sem banco |
| 3 | `application`: `WithdrawService` (`authorize`) + fakes + testes de authorize | 8 min | duplicata e saldo insuficiente verdes |
| 4 | `WithdrawService.confirm` / `.cancel` + tombstone + testes | 7 min | tabela de decisão coberta |
| 5 | `infrastructure`: repositórios, listener, publisher, `WithdrawCommandListenerTest` | 7 min | contexto sobe com `withdraw.country` setado |
| 6 | `application-withdraw-bra.yml` + `withdraw-bra-import.sql` | 3 min | app sobe no ambiente e assina o tópico |
| 7 | `WithdrawFlowIT` | 8 min | demo da fatia roda sozinha |

Passos 1–4 não dependem de Docker: se o Testcontainers estiver lento, a fatia continua avançando.

---

## 11. Demo manual (sem as fatias 1 e 3)

```bash
# terminal 1 — eventos de saída
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic piggies.withdraw.evt --from-beginning

# terminal 2 — comandos de entrada
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic piggies.withdraw.cmd.BRA \
  --property parse.key=true --property key.separator=:
```

```
t-1:{"type":"AuthorizeDebit","transferId":"t-1","account":"BRA-1","amountMinor":100}
t-1:{"type":"AuthorizeDebit","transferId":"t-1","account":"BRA-1","amountMinor":100}   # reentrega: 1 hold só
t-1:{"type":"ConfirmDebit","transferId":"t-1"}
t-2:{"type":"AuthorizeDebit","transferId":"t-2","account":"BRA-1","amountMinor":600}   # DebitRejected
t-3:{"type":"AuthorizeDebit","transferId":"t-3","account":"BRA-1","amountMinor":100}
t-3:{"type":"CancelDebit","transferId":"t-3"}                                          # saldo volta, held=0
```

```sql
select id, balance_minor, held_minor from withdraw_account;
select transfer_id, amount_minor, state from withdraw_hold order by id;
```

---

## 12. KISS → SOLID → DRY: o que **não** construir agora

**KISS agora.** Uma classe de service com três métodos, dois repositórios estreitos, um listener. Regra mora na entidade, orquestração no service, Kafka e JPA só na borda.

**SOLID no refactor** — só quando doer, e provavelmente nem dói nesta hora:
- Extrair uma porta `WithdrawEvents` só faz sentido se surgir um segundo publisher. Hoje o `@KafkaClient` já é a interface, e o service nem depende dela.
- Se o `switch` do listener passar de três casos, aí sim quebrar em handlers.

**DRY só depois de estabilizar.** Deposit (fatia 3) vai parecer 80% igual: mesma forma de idempotência, mesmo shape de `Account`. **Não extraia base comum agora** — a diferença real (`Deposit.Account` não tem `heldMinor`; `PendingCredit` tem dois estados, não três) só fica visível quando as duas fatias estiverem prontas. Uma superclasse `AbstractBalanceService` criada no minuto 20 acopla duas fatias que devem subir em paralelo e quebra as duas de uma vez. Reavalie na integração final, e mesmo lá o custo de manter duplicado costuma ser menor.

**Fora de escopo por decisão explícita:** outbox transacional, transações Kafka/exactly-once, Flyway, retry infinito com backoff, endpoint de cadastro de conta, mapper/DTO entre `contracts` e domínio (o listener converte à mão, são três campos).

---

## 13. Riscos desta fatia

| Risco | Mitigação |
|---|---|
| `micronaut-serde` engasgar com `@JsonSubTypes` | decidir nos primeiros 5 min; plano B é desserializar `String` à mão no listener |
| Mudança em record do `contracts` no meio da hora | só por acordo explícito; campo novo entra opcional, nunca renomeia/remove (SPEC §5) |
| Listener subir no ambiente errado e roubar mensagens | `@Requires(property = "withdraw.country")` em listener e publisher |
| Offset commitado antes do processamento (perda de comando) | `OffsetStrategy.SYNC_PER_RECORD` + `RETRY_ON_ERROR` |
| Reentrega concorrente criando dois holds | unique em `transfer_id` + `SELECT ... FOR UPDATE` na conta |
| `CancelDebit` chegando antes do `AuthorizeDebit` (timeout) | tombstone `Hold(CANCELLED)` |
