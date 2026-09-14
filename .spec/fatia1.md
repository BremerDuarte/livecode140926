# Slice 1 — Coordinator (Dev A)

Async HTTP edge + orchestrated saga. Demonstrable **without** Dev B (Withdraw) and Dev C
(Deposit): `POST /transfers` answers 202, `AuthorizeDebit` appears on the Kafka topic, and
events injected by hand walk `GET /transfers/{id}` to `COMPLETED` / `FAILED` / `COMPENSATED`.

Stack as given: Java 25, Micronaut 5.1.3 (Netty), Hibernate/JPA, PostgreSQL, Kafka, Maven, Lombok.

---

## 0. Prerequisite — the frozen `contracts` package (minute 0)

Shared by all three slices. Once written it changes **only by explicit agreement**; new fields
arrive optional, nothing is renamed or removed during the hour.

```
co.inter.piggies.contracts
├── Topics.java            topic names + per-country routing
├── FailureReasons.java    INSUFFICIENT_FUNDS | ACCOUNT_NOT_FOUND | TIMEOUT
├── WithdrawCommand.java   sealed: AuthorizeDebit | ConfirmDebit | CancelDebit
├── WithdrawEvent.java     sealed: DebitAuthorized | DebitRejected | DebitConfirmed | DebitCancelled
├── DepositCommand.java    sealed: AuthorizeCredit | ConfirmCredit
└── DepositEvent.java      sealed: CreditAuthorized | CreditRejected | CreditConfirmed
```

Six files, because the records nest inside their sealed interface. `amountMinor` is a primitive
**`long`** everywhere — never `Long`, never `double`, never `BigDecimal`.

### Topics

```java
public final class Topics {
    public static final String WITHDRAW_CMD_PREFIX = "piggies.withdraw.cmd.";
    public static final String DEPOSIT_CMD_PREFIX  = "piggies.deposit.cmd.";
    public static final String WITHDRAW_EVT = "piggies.withdraw.evt";
    public static final String DEPOSIT_EVT  = "piggies.deposit.evt";

    private Topics() {}
    public static String withdrawCmd(String country) { return WITHDRAW_CMD_PREFIX + country; }
    public static String depositCmd(String country)  { return DEPOSIT_CMD_PREFIX + country; }
}
```

The prefixes are public constants **on purpose**: Dev B needs `@Topic(Topics.WITHDRAW_CMD_PREFIX + "BRA")`,
and string-constant concatenation is a compile-time constant in Java, so it is legal in an
annotation. A method call would not be.

### Message types

Four event types share `piggies.withdraw.evt` and three share `piggies.deposit.evt`, so the
payload needs a discriminator. A sealed interface with `@JsonTypeInfo`/`@JsonSubTypes` gives a
wire-level `type` field *and* an exhaustive `switch` in the saga:

```java
@Serdeable
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = WithdrawEvent.DebitAuthorized.class, name = "DebitAuthorized"),
    @JsonSubTypes.Type(value = WithdrawEvent.DebitRejected.class,   name = "DebitRejected"),
    @JsonSubTypes.Type(value = WithdrawEvent.DebitConfirmed.class,  name = "DebitConfirmed"),
    @JsonSubTypes.Type(value = WithdrawEvent.DebitCancelled.class,  name = "DebitCancelled")
})
public sealed interface WithdrawEvent {
    String transferId();

    @Serdeable record DebitAuthorized(String transferId) implements WithdrawEvent {}
    @Serdeable record DebitRejected(String transferId, String reason) implements WithdrawEvent {}
    @Serdeable record DebitConfirmed(String transferId) implements WithdrawEvent {}
    @Serdeable record DebitCancelled(String transferId) implements WithdrawEvent {}
}
```

Same shape for the other three; `AuthorizeDebit(transferId, account, amountMinor)` and
`AuthorizeCredit(transferId, account, amountMinor)` are the only ones carrying money.

**This is verified, not assumed.** The project uses `micronaut-serde-jackson` (Micronaut Serde,
not classic Jackson databind), and `micronaut-serde-processor-3.1.1.jar` ships
`JsonTypeInfoMapper`, `JsonSubTypesMapper` and `JsonTypeNameMapper`, which map these annotations
onto `SerdeConfig$SerSubtyped`. `jackson-annotations` 2.21 is on the compile classpath as a
transitive `compile` dependency. Every record still needs its own `@Serdeable`.

On the wire: `{"type":"DebitAuthorized","transferId":"..."}` — short enough to type into
`kafka-console-producer`, which is the entire point of the slice-1 demo.

> **De-risking fallback.** If the annotation processor misbehaves at minute 5, collapse each
> sealed interface into one flat record with a nested `enum Type` as its first field
> (`record WithdrawEvent(Type type, String transferId, @Nullable String reason)`). The wire
> format is byte-identical, `switch` over the enum stays exhaustive, and the contract stays
> frozen. Decide this in the first five minutes, never later.

---

## 1. Package layout — the coordinator feature

```
co.inter.piggies.coordinator
├── domain/
│   ├── Transfer.java           @Entity — the aggregate
│   ├── TransferStatus.java     PENDING | DEBIT_AUTHORIZED | CREDIT_AUTHORIZED | COMPLETED | FAILED | COMPENSATED
│   ├── SagaStep.java           the saga's control variable (7 values, below)
│   ├── SagaDecision.java       record(boolean changed, status, step, failureReason, withdrawCommands, depositCommands)
│   └── TransferSaga.java       the state machine — pure, zero framework imports
├── application/
│   ├── TransferService.java    submit / find / onWithdrawEvent / onDepositEvent / applyTimeout
│   └── TimeoutSweeper.java     @Scheduled trigger + sweep(Instant deadline)
└── infrastructure/
    ├── http/TransferController.java     + SubmitRequest / SubmitResponse / TransferView records
    ├── persistence/TransferRepository.java
    └── messaging/SagaCommandPublisher.java, SagaEventListener.java
```

**The one layering rule that is actually enforced:** `domain` imports nothing from
`application` or `infrastructure`, and nothing from Micronaut. `application` imports
`infrastructure` types directly — no `Port`/`Adapter` interfaces, no repository wrapper over
Micronaut Data, no generic saga engine. There is one implementation of each; abstractions wait
until a second one exists.

`domain` does depend on `contracts` (plain records, zero runtime deps) so the state machine can
pattern-match wire types directly. That is the single deliberate compromise, and it is what buys
the design its simplicity.

---

## 2. Domain layer

### `Transfer`

```java
@Entity @Table(name = "transfers")
public class Transfer {
    @Id private String id;                       // UUID string — same type as the Kafka key
    private String fromAccount, fromCountry, toAccount, toCountry;
    private long amountMinor;
    @Enumerated(STRING) private TransferStatus status;
    @Enumerated(STRING) private SagaStep step;
    private String failureReason;
    @Column(unique = true, nullable = false, updatable = false) private String idempotencyKey;
    private Instant createdAt, updatedAt;
    @Version private long version;               // optimistic locking — see §4
}
```

`hbm2ddl=update` creates the UNIQUE index from `@Column(unique = true)`. **The database
constraint is the source of truth for idempotency**; the lookup in `submit()` is only the fast
path.

`failureReason` stays a `String`, not an enum: the text originates downstream
(`DebitRejected{reason}`) and an enum here would couple the Coordinator to Withdraw's
vocabulary. The exact strings live in `contracts.FailureReasons` so all three devs agree; the
only value the Coordinator writes itself is `TIMEOUT`.

### `status` vs `step` — the key design decision

- **`status`** is the coarse, client-facing milestone. Its six values are frozen by the API
  contract. The saga **writes** it and **never reads** it.
- **`step`** is the saga's internal "what am I waiting for". Every guard reads `step`. It is the
  single control variable.

This split lets the saga express states that `status` cannot name. After `CreditRejected` we are
compensating — there is no `COMPENSATING` status — so `status` stays `DEBIT_AUTHORIZED`
(truthfully: the debit *is* still authorized) while `step = AWAITING_DEBIT_CANCELLATION` drives
behaviour.

```java
public enum SagaStep {
    AWAITING_DEBIT_AUTHORIZATION,
    AWAITING_CREDIT_AUTHORIZATION,
    AWAITING_BOTH_CONFIRMATIONS,
    AWAITING_DEBIT_CONFIRMATION,    // credit already confirmed
    AWAITING_CREDIT_CONFIRMATION,   // debit already confirmed
    AWAITING_DEBIT_CANCELLATION,
    DONE
}
```

**"Both confirmations arrived" needs no extra columns.** `AWAITING_BOTH_CONFIRMATIONS` fans out
into two "one down, one to go" steps. Four extra rows in the transition table, zero extra
schema, and it works regardless of arrival order. Rejected alternatives: two booleans (three
pieces of state to keep consistent instead of one), a bitmask (unreadable in `psql`, which is
exactly where you will be debugging during a demo).

### `TransferSaga` — the state machine

The unit the spec explicitly demands unit tests for. No Kafka, no JPA session, no Micronaut
context: a test builds a `Transfer` with `new`, calls a static method, asserts the returned
decision.

```java
public final class TransferSaga {
    private TransferSaga() {}

    public static SagaDecision start(Transfer t);
    public static SagaDecision on(Transfer t, WithdrawEvent e);
    public static SagaDecision on(Transfer t, DepositEvent e);
    public static SagaDecision onTimeout(Transfer t);
}
```

`SagaDecision` carries two typed lists rather than one `List<Object>`, so the publisher's routing
stays type-safe. Exactly one transition ever fills both.

```java
public record SagaDecision(boolean changed, TransferStatus status, SagaStep step,
                           @Nullable String failureReason,
                           List<WithdrawCommand> withdrawCommands,
                           List<DepositCommand> depositCommands) {

    public static SagaDecision ignore(Transfer t) {
        return new SagaDecision(false, t.getStatus(), t.getStep(), t.getFailureReason(), List.of(), List.of());
    }
}
```

#### Transition table

Every rule guards on the **current `step`**. Any (step, event) pair not listed returns
`ignore()` — no state change, no commands. That single rule is simultaneously the guard against
at-least-once redelivery, against out-of-order arrival, and against late events after a terminal
state.

**`start()`** — called once, immediately after the row is inserted:

| step.in | → status | → step | emits |
|---|---|---|---|
| *(new)* | PENDING | AWAITING_DEBIT_AUTHORIZATION | `AuthorizeDebit` → `withdrawCmd(fromCountry)` |

**`on(t, WithdrawEvent)`**:

| step.in | event | → status | → step | failureReason | emits |
|---|---|---|---|---|---|
| AWAITING_DEBIT_AUTHORIZATION | `DebitAuthorized` | DEBIT_AUTHORIZED | AWAITING_CREDIT_AUTHORIZATION | — | `AuthorizeCredit` → `depositCmd(toCountry)` |
| AWAITING_DEBIT_AUTHORIZATION | `DebitRejected` | **FAILED** | DONE | `e.reason()` | **nothing** — Deposit is never contacted |
| AWAITING_BOTH_CONFIRMATIONS | `DebitConfirmed` | *(unchanged)* | AWAITING_CREDIT_CONFIRMATION | — | — |
| AWAITING_DEBIT_CONFIRMATION | `DebitConfirmed` | **COMPLETED** | DONE | — | — |
| AWAITING_DEBIT_CANCELLATION | `DebitCancelled` | `TIMEOUT` ? **FAILED** : **COMPENSATED** | DONE | *unchanged* | — |
| DONE + FAILED/`TIMEOUT` | `DebitAuthorized` | *(unchanged)* | *(unchanged)* | *unchanged* | `CancelDebit` → `withdrawCmd(fromCountry)` |
| *anything else* | *any* | — | — | — | `ignore()` |

**`on(t, DepositEvent)`**:

| step.in | event | → status | → step | failureReason | emits |
|---|---|---|---|---|---|
| AWAITING_CREDIT_AUTHORIZATION | `CreditAuthorized` | CREDIT_AUTHORIZED | AWAITING_BOTH_CONFIRMATIONS | — | `ConfirmDebit` **and** `ConfirmCredit` |
| AWAITING_CREDIT_AUTHORIZATION | `CreditRejected` | *(unchanged)* | AWAITING_DEBIT_CANCELLATION | `e.reason()` | `CancelDebit` → `withdrawCmd(fromCountry)` |
| AWAITING_BOTH_CONFIRMATIONS | `CreditConfirmed` | *(unchanged)* | AWAITING_DEBIT_CONFIRMATION | — | — |
| AWAITING_CREDIT_CONFIRMATION | `CreditConfirmed` | **COMPLETED** | DONE | — | — |
| *anything else* | *any* | — | — | — | `ignore()` |

**`onTimeout(t)`**:

| step.in | → status | → step | failureReason | emits |
|---|---|---|---|---|
| AWAITING_DEBIT_AUTHORIZATION | **FAILED** | DONE | `TIMEOUT` | nothing (nothing was authorized) |
| AWAITING_CREDIT_AUTHORIZATION | *(unchanged)* | AWAITING_DEBIT_CANCELLATION | `TIMEOUT` | `CancelDebit` |
| AWAITING_BOTH_CONFIRMATIONS / AWAITING_DEBIT_CONFIRMATION / AWAITING_CREDIT_CONFIRMATION | **IGNORED** | | | |
| AWAITING_DEBIT_CANCELLATION / DONE | ignored | | | |

Three rules in that table are load-bearing and easy to get wrong:

1. **The confirmation phase never times out.** SPEC §5: *"Se `ConfirmCredit` falhar após
   `ConfirmDebit`, não desfaço"*. A timeout that fired at `AWAITING_BOTH_CONFIRMATIONS` would
   emit `CancelDebit` against a debit that was already confirmed — a phantom credit, money
   created from nothing. This is the single most dangerous transition in the slice, and the
   correct behaviour is to do nothing.
2. **A late `DebitAuthorized` after a `TIMEOUT` failure still emits `CancelDebit`.** The timeout
   fired precisely because we did not know whether the authorization had happened; the late
   event is the only evidence that it did. Ignoring it leaves `heldMinor > 0` on the source
   account forever, which is exactly what criterion 8's *"compensação do que já foi autorizado"*
   forbids. `status` stays `FAILED` — this is cleanup, not a state transition.
3. **`DebitCancelled` resolves to `FAILED` when the reason was `TIMEOUT`, `COMPENSATED`
   otherwise.** See the ambiguity note in §10.

#### Shape of the code

```java
public static SagaDecision on(Transfer t, WithdrawEvent e) {
    return switch (e) {
        case WithdrawEvent.DebitAuthorized ignored -> switch (t.getStep()) {
            case AWAITING_DEBIT_AUTHORIZATION -> new SagaDecision(true,
                    TransferStatus.DEBIT_AUTHORIZED, SagaStep.AWAITING_CREDIT_AUTHORIZATION, null,
                    List.of(), List.of(new DepositCommand.AuthorizeCredit(t.getId(), t.getToAccount(), t.getAmountMinor())));
            // late authorization for a transfer we already gave up on: release the hold
            case DONE -> isTimedOut(t)
                    ? new SagaDecision(false, t.getStatus(), t.getStep(), t.getFailureReason(),
                            List.of(new WithdrawCommand.CancelDebit(t.getId())), List.of())
                    : SagaDecision.ignore(t);
            default -> SagaDecision.ignore(t);
        };
        case WithdrawEvent.DebitRejected r -> ...
        case WithdrawEvent.DebitConfirmed ignored -> ...
        case WithdrawEvent.DebitCancelled ignored -> ...
    };
}
```

Sealed interface + `switch` expression means **no `default` on the outer switch** and a compile
error the moment someone adds a message type without handling it.

---

## 3. Application layer

### `TransferService.submit` — idempotency

```java
public Transfer submit(SubmitCommand cmd) {
    Optional<Transfer> existing = repository.findByIdempotencyKey(cmd.idempotencyKey());
    if (existing.isPresent()) return existing.get();          // sequential retry: no emit

    Transfer t = Transfer.pending(UUID.randomUUID().toString(), cmd, Instant.now());
    Transfer saved;
    try {
        saved = repository.save(t);                            // own tx; flushes + commits here
    } catch (PersistenceException race) {                      // UNIQUE violation
        return repository.findByIdempotencyKey(cmd.idempotencyKey())
                         .orElseThrow(() -> race);             // fresh tx, fresh session
    }
    publisher.publish(saved, TransferSaga.start(saved));       // only the winner emits
    return saved;
}
```

> **`submit()` must NOT be `@Transactional`.** This is the trap in this method. Under
> `@Transactional` the INSERT is deferred to commit, which happens inside the interceptor
> *after* your method returns — so the `catch` never fires; and if you force a flush to make it
> fire, the session is poisoned, the transaction is rollback-only, and the re-read throws
> `UnexpectedRollbackException`. Micronaut Data repository methods are individually
> transactional (`HibernateJpaOperations` wraps each write in `executeWrite`), so leaving
> `submit()` non-transactional makes the failed INSERT roll back its own small transaction and
> the re-read run in a clean one.

The re-read discriminates better than any exception-type check, so you never have to be right
about the exception subtype. Catch `jakarta.persistence.PersistenceException` — Hibernate's
`ConstraintViolationException` extends it. **Import warning:** `jakarta.validation.ConstraintViolationException`
is also on the classpath and your IDE will offer it first.

The loser's INSERT *blocks* on the unique index until the winner commits, so by the time the
exception arrives the winner's row is committed and visible. No retry loop needed.

A repeat POST returns the transfer's **current** status, not a hard-coded `PENDING` — a
duplicate POST three seconds later should not report `PENDING` for a `COMPLETED` transfer.

### `TransferService.onWithdrawEvent` / `onDepositEvent`

Load by id → run the saga → if `changed()`, `update` + bump `updatedAt` → publish the commands.

An event for an **unknown `transferId`** (replayed topic, typo in the demo) logs at WARN and
returns normally so the offset commits. **Never throw** — with `RETRY_ON_ERROR` configured, a
throw wedges the partition.

### `TimeoutSweeper`

```java
@Scheduled(fixedDelay = "${piggies.coordinator.sweep-interval:1s}")
void sweepNow() { sweep(Instant.now().minus(sagaTimeout)); }

public void sweep(Instant deadline) {                          // deterministic entry point
    for (String id : repository.findExpired(deadline)) service.applyTimeout(id);
}
```

`fixedDelay`, never `fixedRate` — `fixedDelay` cannot overlap itself.

```java
@Query("select t.id from Transfer t where t.step in (:awaitingDebit, :awaitingCredit) and t.updatedAt < :deadline")
List<String> findExpired(SagaStep awaitingDebit, SagaStep awaitingCredit, Instant deadline);
```

Explicit JPQL rather than gambling on derived-query keyword spelling during a live code. The
`step in (...)` clause is belt-and-braces with the saga's own guard: **only the two
authorization steps are ever candidates**, so the confirmation phase cannot time out even if
someone later loosens `onTimeout`.

**Determinism without a `Clock` bean.** Three layers, none of which sleep:

1. `TransferSaga.onTimeout(t)` takes no clock at all — timeout enters the domain as an explicit
   method call, exactly like an event. Every timeout case is a pure, instant unit test.
2. `sweep(Instant deadline)` takes the deadline as a parameter. A test calls
   `sweeper.sweep(Instant.now().plusSeconds(60))` to declare everything expired *right now*.
3. The window is config-driven, so the integration test sets `saga-timeout: 500ms`.

Injecting a `java.time.Clock` was considered and rejected: Micronaut has no default `Clock`
bean, so it needs a `@Factory` plus a `@Replaces` fake, and it buys nothing the deadline
parameter does not already buy.

---

## 4. Infrastructure layer

### `TransferController`

```java
@Controller("/transfers")
@ExecuteOn(TaskExecutors.BLOCKING)     // virtual threads on JDK 25
public class TransferController { ... }
```

`@ExecuteOn(TaskExecutors.BLOCKING)` is **not optional**: the handler does JDBC and a Kafka send,
neither of which may run on a Netty event loop, and the `< 100ms` criterion is measured with
concurrent requests in flight.

- `POST /transfers` → `202` `{transferId, status}`. Bean validation (`@NotBlank`,
  `@Positive long amountMinor`); `micronaut-problem-json` is already on the classpath and renders
  the `400 application/problem+json` with no hand-written exception handler.
- `GET /transfers/{transferId}` → `200` `{transferId, status, step, failureReason?, updatedAt}`,
  `404` for an unknown id.

Request-thread budget: validation (µs) → indexed `SELECT` by `idempotency_key` (~1 ms) →
`INSERT` + commit (~2–3 ms) → produce `AuthorizeDebit` (~2–5 ms local) → return 202. **Zero
synchronous calls to Withdraw or Deposit.** If the send ever needs to leave the critical path,
change the `@KafkaClient` method's return type to `CompletableFuture<Void>` — the interceptor
then does not block. Do not hand-roll a thread pool.

### `TransferRepository`

```java
@Repository
public interface TransferRepository extends CrudRepository<Transfer, String> {
    Optional<Transfer> findByIdempotencyKey(String idempotencyKey);

    @Query("select t.id from Transfer t where t.step in (:a, :b) and t.updatedAt < :deadline")
    List<String> findExpired(SagaStep a, SagaStep b, Instant deadline);
}
```

### `SagaCommandPublisher`

```java
@Singleton
public class SagaCommandPublisher {

    public void publish(Transfer t, SagaDecision d) {
        for (WithdrawCommand c : d.withdrawCommands()) client.send(Topics.withdrawCmd(t.getFromCountry()), t.getId(), c);
        for (DepositCommand  c : d.depositCommands())  client.send(Topics.depositCmd(t.getToCountry()),   t.getId(), c);
    }

    @KafkaClient(id = "coordinator-commands")
    interface Client {
        void send(@Topic String topic, @KafkaKey String transferId, WithdrawCommand command);
        void send(@Topic String topic, @KafkaKey String transferId, DepositCommand  command);
    }
}
```

**The dynamic topic is verified.** `@Topic`'s `@Target` includes `ElementType.PARAMETER`, and
`KafkaClientIntroductionAdvice$ProducerState` resolves the topic from
`ctx.getParameterValues()[index]` when non-empty, falling back to the annotation-derived topic
otherwise. The country comes from the **transfer row**, so there is no routing table to
configure. Nesting the interface inside the publisher saves a file and mirrors the
`KafkaContainerTest.PiggyProducer` idiom already in the repo.

### `SagaEventListener`

```java
@KafkaListener(groupId = "piggies-coordinator",
               offsetReset = OffsetReset.EARLIEST,
               threads = 1,
               consumerCreationStrategy = ConsumerCreationStrategy.PER_CLASS,
               errorStrategy = @ErrorStrategy(value = ErrorStrategyValue.RETRY_ON_ERROR,
                                              retryCount = 5, retryDelay = "100ms"))
public class SagaEventListener {
    @Topic(Topics.WITHDRAW_EVT) void onWithdraw(WithdrawEvent e) { service.onWithdrawEvent(e); }
    @Topic(Topics.DEPOSIT_EVT)  void onDeposit(DepositEvent e)   { service.onDepositEvent(e); }
}
```

Every one of those four attributes is load-bearing:

- **`offsetReset = EARLIEST`.** The default is `LATEST`, which is the single most likely thing to
  kill the demo: a brand-new consumer group silently skips everything produced before partition
  assignment, and "I pasted `DebitAuthorized` into the console producer and nothing happened" eats
  five minutes on a shared screen.
- **`consumerCreationStrategy = PER_CLASS` + `threads = 1`.** The default is `PER_TOPIC`, which
  builds **one consumer and one polling thread per `@Topic` method** even inside a single
  listener class. `DebitConfirmed` (withdraw.evt) and `CreditConfirmed` (deposit.evt) would then
  be processed concurrently against the same `Transfer` row — a genuine lost update, after which
  the transfer waits forever for a confirmation that already arrived. `PER_CLASS` puts both
  topics on one consumer thread in arrival order. Document it as a deliberate simplification: a
  second coordinator instance would reintroduce the race.
- **`@Version` + `RETRY_ON_ERROR`.** The listener is not the only writer — `TimeoutSweeper` is a
  second one. Optimistic locking catches that collision and the retry re-reads the fresh state,
  where the saga's step guard turns the stale trigger into `ignore()`.

### Kafka send vs. database commit

**Send strictly after commit**, on both the POST path and the event path. Since nothing in the
coordinator is `@Transactional`, "after commit" is simply "on the next line after `save`/`update`
returns" — no `TransactionalEventListener`, no outbox.

| Ordering | Failure mode |
|---|---|
| **commit → send** *(chosen)* | Crash between them: the row sits in `PENDING` with no command emitted, and the 5 s sweeper moves it to `FAILED`/`TIMEOUT`. Self-healing, no money moved. |
| send → commit *(rejected)* | Send succeeds, tx rolls back: `AuthorizeDebit` exists for a `transferId` the coordinator has no row for. Dev B creates a `Hold` and emits `DebitAuthorized`; the coordinator sees an unknown id and ignores it. **Funds held forever with no owner.** |

The failure modes are not symmetric, which is what settles it. On the POST path, catch a failed
send and **still return 202** — the row is committed and the sweeper owns it; turning a durable
`PENDING` into an HTTP 500 breaks criterion 1 for no benefit.

**No transactional outbox in this slice.** It buys exactly the one case the timeout already
covers, and costs an outbox table, a relay poller, and ordering/dedupe concerns. Say the
sentence out loud during the demo instead: *"no outbox — the 5 s timeout is the recovery
mechanism for a lost command; in production an outbox + relay replaces it."*

---

## 5. Configuration

One artifact, three environments (SPEC §5: *"mesmo artefato Maven, ambientes `coordinator |
withdraw-bra | deposit-usa`"*).

**`src/main/resources/application.yml`** — drop the hard-coded `datasources.default` block. It
belongs to whichever environment is active; leaving a localhost datasource in the shared file
means three devs fighting over one database. Keep:

```yaml
micronaut:
  application:
    name: livecode140926
jpa:
  default:
    properties:
      hibernate:
        hbm2ddl:
          auto: update
kafka:
  bootstrap:
    servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
  producers:
    default:
      acks: all
      enable.idempotence: true
```

**`src/main/resources/application-coordinator.yml`** (new):

```yaml
micronaut:
  server:
    port: 8080
datasources:
  default:
    url: jdbc:postgresql://localhost:5432/coordinator
    username: piggies
    password: piggies
    driver-class-name: org.postgresql.Driver
    db-type: postgres
    dialect: POSTGRES
jpa:
  default:
    entity-scan:
      packages:
        - co.inter.piggies.coordinator.domain
piggies:
  coordinator:
    saga-timeout: 5s
    sweep-interval: 1s
```

`jpa.default.entity-scan.packages` is the line that makes *"instância isolada por país"* real
rather than aspirational: even though B's and C's entities ship in the same jar, the
coordinator's persistence unit only ever knows about `Transfer`.

Run: `./mvnw mn:run -Dmicronaut.environments=coordinator`.

> **Do not add `@Requires(env = "coordinator")` in slice 1.** Right now the coordinator classes
> are the only ones in the jar, and a forgotten `-Dmicronaut.environments` flag silently
> producing an app with no listeners is exactly the kind of thing that eats five minutes on a
> shared screen. Add it in the integration slice, on precisely two beans per service.

---

## 6. Build changes

1. Add to `pom.xml`, test scope, **no `<version>`** — the platform BOM manages Mockito 5.23.0:
   ```xml
   <dependency>
     <groupId>org.mockito</groupId>
     <artifactId>mockito-junit-jupiter</artifactId>
     <scope>test</scope>
   </dependency>
   ```
2. On JDK 25 Mockito's inline mock maker self-attaches its agent and prints a warning. If the
   noise is distracting, add `-XX:+EnableDynamicAgentLoading` to the surefire `argLine`.
3. **Delete `src/test/java/co/inter/piggies/support/Piggy.java`** — it exists only because
   Hibernate refuses to build a `SessionFactory` with zero entities, and its own Javadoc says to
   delete it once `src/main` has entities. `Transfer` is that entity.
4. **Then fix `src/test/java/co/inter/piggies/PostgresContainerTest.java`**, whose
   `hibernateCreatedItsSchemaInTheContainer()` asserts `table_name = 'piggy'`. Point it at
   `'transfers'` — the assertion becomes *more* valuable, since it now proves the real schema was
   created. Nothing else references `Piggy`.

   Sequence steps 3–4 *after* `Transfer` compiles and the context starts, not before — otherwise
   there is a window in which no test can start an application context at all.

---

## 7. Tests

### Unit — no containers, no Micronaut context, milliseconds

**`TransferSagaTest`** — the centrepiece. The saga is a pure static function, so this class needs
no mocks at all.

| `@Nested` group | Cases |
|---|---|
| `Start` | emits exactly one `AuthorizeDebit` to `piggies.withdraw.cmd.BRA` carrying `fromAccount` + `amountMinor`; no deposit command |
| `DebitAuthorization` | `DebitAuthorized` → `DEBIT_AUTHORIZED` + one `AuthorizeCredit` to `...deposit.cmd.USA` carrying `toAccount`; `DebitRejected` → `FAILED`/`INSUFFICIENT_FUNDS` and **zero deposit commands** *(criterion 3)* |
| `CreditAuthorization` | `CreditAuthorized` → `AWAITING_BOTH_CONFIRMATIONS` emitting **both** `ConfirmDebit` and `ConfirmCredit`; `CreditRejected` → `AWAITING_DEBIT_CANCELLATION` + exactly one `CancelDebit`, reason recorded |
| `Confirmations` | debit-then-credit → `COMPLETED`; credit-then-debit → `COMPLETED`; each intermediate step asserted; **neither confirmation alone completes** |
| `Compensation` | `DebitCancelled` at `AWAITING_DEBIT_CANCELLATION` → `COMPENSATED`, `failureReason` preserved |
| `Timeout` | at `AWAITING_DEBIT_AUTHORIZATION` → `FAILED`/`TIMEOUT`, **no commands**; at `AWAITING_CREDIT_AUTHORIZATION` → `CancelDebit` + `TIMEOUT`; **ignored at all three confirmation steps** *(the money-safety rule)*; ignored at `DONE` |
| `LateEvents` | `DebitAuthorized` after a `TIMEOUT` failure → emits `CancelDebit`, status stays `FAILED` *(criterion 8, second half)* |
| `Redelivery` | duplicate `DebitAuthorized` emits **one** `AuthorizeCredit`, not two *(criterion 5)*; duplicate `CreditConfirmed` after `COMPLETED` changes nothing |
| `Totality` | `@ParameterizedTest` over the full (step × event) cross-product: `decide` never throws and never moves a `DONE` transfer. Cheapest possible insurance against a forgotten switch arm. |

**`TransferServiceTest`** (Mockito) — second `submit` with the same `idempotencyKey` returns the
same `transferId` and publishes **nothing** the second time *(criterion 6)*; a repository stubbed
to throw `PersistenceException` exercises the catch-and-reread branch and returns the winner's
id; an event for an unknown `transferId` neither throws nor publishes; a decision with
`changed() == false` neither updates nor publishes.

**`TimeoutSweeperTest`** (Mockito) — `sweep(deadlineInTheFuture)` transitions every expired
transfer; `sweep(deadlineInThePast)` does nothing; a transfer at `AWAITING_CREDIT_AUTHORIZATION`
produces exactly one `CancelDebit`. Zero `Thread.sleep`.

### Integration — `CoordinatorFlowTest extends AbstractContainerTest`

Named `*Test`, not `*IT`: the project declares no failsafe plugin, so surefire is what runs.
Reuses the pinned singletons in `co.inter.piggies.support.TestContainers` and the
`new HashMap<>(super.getProperties())` idiom from `KafkaContainerTest`.

Override `getProperties()` to (a) pre-create all four topics via `TestContainers.createTopic` —
the only window before the listeners subscribe — and (b) set `piggies.coordinator.saga-timeout`.
A test-only `@KafkaListener` on `piggies.withdraw.cmd.BRA` and `piggies.deposit.cmd.USA` stands
in for `kafka-console-consumer`; a `@KafkaClient` stands in for `kafka-console-producer`.
Assertions via Awaitility, already in the pom.

Covers: `POST` → 202 `PENDING`; `AuthorizeDebit` observed; the happy path walked to `COMPLETED`;
`DebitRejected` → `FAILED`/`INSUFFICIENT_FUNDS` with **no deposit command ever produced**; two
POSTs with one `idempotencyKey` → same `transferId`.

> **Gotcha:** set `saga-timeout` to something long (60s) for the event-driven tests. At the
> production 5 s the sweeper will fail the transfer out from under the test while it is
> hand-feeding events, and it will flake. Give the timeout test its own class or its own short
> value (500ms).

---

## 8. Demo — slice 1 standing alone

> Start with a long window so the sweeper does not fire while you type, then demo the timeout
> deliberately as its own step:
> `./mvnw mn:run -Dmicronaut.environments=coordinator -Dpiggies.coordinator.saga-timeout=10m`

### Setup

```bash
docker run -d --name piggies-kafka -p 9092:9092 apache/kafka-native:4.3.1
docker run -d --name piggies-pg -p 5432:5432 \
  -e POSTGRES_DB=coordinator -e POSTGRES_USER=piggies -e POSTGRES_PASSWORD=piggies postgres:16-alpine

for T in piggies.withdraw.cmd.BRA piggies.deposit.cmd.USA piggies.withdraw.evt piggies.deposit.evt; do
  docker exec piggies-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$T" --partitions 1 --replication-factor 1
done
```

Two terminals stand in for Dev B and Dev C:

```bash
docker exec -it piggies-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic piggies.withdraw.cmd.BRA --from-beginning   # T1
docker exec -it piggies-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic piggies.deposit.cmd.USA --from-beginning    # T2
```

### A — happy path to `COMPLETED`

```bash
curl -si -X POST localhost:8080/transfers -H 'Content-Type: application/json' -d '{
  "fromAccount":"BR-1","fromCountry":"BRA",
  "toAccount":"US-1","toCountry":"USA",
  "amountMinor":10000,"idempotencyKey":"demo-happy-1"}'
```

→ `202 Accepted`, `{"transferId":"<ID>","status":"PENDING"}`.
**T1 prints** `{"type":"AuthorizeDebit","transferId":"<ID>","account":"BR-1","amountMinor":10000}`.

```bash
export ID=<paste>
P() { docker exec -i piggies-kafka /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server localhost:9092 --topic "$1"; }

echo "{\"type\":\"DebitAuthorized\",\"transferId\":\"$ID\"}"  | P piggies.withdraw.evt
#   -> T2 prints AuthorizeCredit;  GET -> DEBIT_AUTHORIZED / AWAITING_CREDIT_AUTHORIZATION

echo "{\"type\":\"CreditAuthorized\",\"transferId\":\"$ID\"}" | P piggies.deposit.evt
#   -> T1 prints ConfirmDebit AND T2 prints ConfirmCredit;  GET -> CREDIT_AUTHORIZED

echo "{\"type\":\"DebitConfirmed\",\"transferId\":\"$ID\"}"   | P piggies.withdraw.evt
echo "{\"type\":\"CreditConfirmed\",\"transferId\":\"$ID\"}"  | P piggies.deposit.evt

curl -s localhost:8080/transfers/$ID    # -> {"status":"COMPLETED","step":"DONE",...}
```

Run it a second time with the two confirmations in the **opposite order** — same result. That is
the order-independence of `AWAITING_BOTH_CONFIRMATIONS`, demoed.

### B — `FAILED` / `INSUFFICIENT_FUNDS` *(criterion 3)*

New POST, then `{"type":"DebitRejected","transferId":"<ID2>","reason":"INSUFFICIENT_FUNDS"}` on
`piggies.withdraw.evt` → `GET` shows `FAILED` / `INSUFFICIENT_FUNDS`, and **T2 stays silent** —
Deposit was never contacted.

### C — `COMPENSATED` *(criterion 4)*

New POST → `DebitAuthorized` → `{"type":"CreditRejected",...,"reason":"ACCOUNT_NOT_FOUND"}` on
`piggies.deposit.evt` → **T1 prints `CancelDebit`**; reply `DebitCancelled` → `COMPENSATED`,
`failureReason` still `ACCOUNT_NOT_FOUND`.

### D — idempotency *(criteria 5 and 6)*

The exact same POST twice → identical `transferId`, and **T1 shows exactly one `AuthorizeDebit`**.
Then replay one `DebitAuthorized` twice → **T2 shows exactly one `AuthorizeCredit`**.

### E — timeout *(criterion 8)*

Restart with the real 5 s window. POST and do nothing → after 6 s, `FAILED` / `TIMEOUT`.
Compensating variant: POST, send only `DebitAuthorized`, wait → **T1 prints `CancelDebit`**.

### F — `400 problem+json` *(criterion 1)*

```bash
curl -si -X POST localhost:8080/transfers -H 'Content-Type: application/json' \
  -d '{"fromAccount":"","fromCountry":"BRA","toAccount":"US-1","toCountry":"USA","amountMinor":-1,"idempotencyKey":"x"}'
```

---

## 9. Order of work

1. **`contracts`** — all six files, with Dev B and Dev C, then frozen. *(~10 min, together)*
2. **`Transfer` + `TransferStatus` + `SagaStep`** → compile and start the context once to prove
   Hibernate is happy, then delete `Piggy` and fix `PostgresContainerTest`.
3. **`TransferSaga` + `TransferSagaTest`** — the slice's centre of gravity, and it needs no
   infrastructure. Write it before any wiring.
4. **Repository → `SagaCommandPublisher` → `TransferService.submit` → `TransferController`.**
   First demoable milestone: demo A step 1 (POST → 202 → `AuthorizeDebit` in the console consumer).
5. **`SagaEventListener` + the event handlers.** Demos A, B, C.
6. **`TimeoutSweeper` + `TimeoutSweeperTest`.** Demo E.
7. **`TransferServiceTest`**, then `CoordinatorFlowTest` if time allows.

---

## 10. Ambiguities resolved, and what is deliberately deferred

**Resolved by decision:**

1. **Timeout end state.** §3 says `FAILED` + `TIMEOUT` **plus** compensation, while the other
   criterion ends a compensated saga at `COMPENSATED`. Chosen: `failureReason = TIMEOUT` always;
   the terminal status is `FAILED` when the timeout caused it and `COMPENSATED` when
   `CreditRejected` did. One ternary in the `DebitCancelled` arm, and both criteria read
   literally true.
2. **Is the 5 s window per-step or end-to-end?** Chosen: **per step**, keyed on `updatedAt`.
   *"Autorização sem resposta em 5s"* reads as per-authorization. An end-to-end deadline would key
   on `createdAt` — a one-word change to the JPQL if you disagree.
3. **Does the confirmation phase time out?** Chosen: **no.** §5 wins over §3 because reverting
   there creates phantom credit. See §2.
4. **Who owns the string `INSUFFICIENT_FUNDS`?** The Coordinator copies `event.reason()` verbatim;
   the constants live in the frozen `contracts.FailureReasons` so Dev B and the Coordinator agree
   with zero mapping logic.
5. **Repeat POST: 202 or 200?** Chosen: **202** both times, with the transfer's current status.
6. **`GET` on an unknown id?** Unspecified. Chosen: **404**.
7. **Event for an unknown `transferId`?** Chosen: log WARN, commit the offset, do nothing. Never
   throw, or `RETRY_ON_ERROR` wedges the partition.
8. **`status` during the confirmation phase.** The frozen enum has no `CONFIRMING`, so `status`
   stays `CREDIT_AUTHORIZED` across all three confirmation steps and `step` carries the detail —
   which is exactly why `step` is in the `GET` contract.

**Deferred on purpose:**

- **Transactional outbox** — the 5 s timeout covers the lost-command case (§4).
- **Kafka exactly-once** — SPEC excludes it; every write is idempotent by `transferId`.
- **Re-emitting `CancelDebit` when `DebitCancelled` never arrives** — `CancelDebit` is idempotent
  at Withdraw so re-emitting is *safe*, it is simply not what makes criterion 4 pass. Log a WARN
  when a transfer has been awaiting cancellation for too long; build the backoff loop later.
- **FX** — 1:1 parity, same `amountMinor` on both ends.
- **Flyway** — `hbm2ddl=update` from the skeleton stays.
- **`@Requires(env = ...)` on the listeners and sweeper** — added in the integration slice (§5).
