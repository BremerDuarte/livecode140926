package co.inter.piggies.coordinator.application;

import co.inter.piggies.contracts.DepositEvent;
import co.inter.piggies.contracts.WithdrawEvent;
import co.inter.piggies.coordinator.domain.SagaDecision;
import co.inter.piggies.coordinator.domain.Transfer;
import co.inter.piggies.coordinator.domain.TransferSaga;
import co.inter.piggies.coordinator.infrastructure.messaging.SagaCommandPublisher;
import co.inter.piggies.coordinator.infrastructure.persistence.TransferRepository;
import jakarta.inject.Singleton;
import jakarta.persistence.PersistenceException;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class TransferService {

    private final TransferRepository repository;
    private final SagaCommandPublisher publisher;

    public TransferService(TransferRepository repository, SagaCommandPublisher publisher) {
        this.repository = repository;
        this.publisher = publisher;
    }

    public record SubmitCommand(String fromAccount, String fromCountry, String toAccount, String toCountry,
                                 long amountMinor, String idempotencyKey) {}

    public Transfer submit(SubmitCommand cmd) {
        Optional<Transfer> existing = repository.findByIdempotencyKey(cmd.idempotencyKey());
        if (existing.isPresent()) {
            return existing.get();
        }

        Transfer t = Transfer.pending(UUID.randomUUID().toString(), cmd.fromAccount(), cmd.fromCountry(),
                cmd.toAccount(), cmd.toCountry(), cmd.amountMinor(), cmd.idempotencyKey(), Instant.now());
        Transfer saved;
        try {
            saved = repository.save(t);
        } catch (PersistenceException race) {
            return repository.findByIdempotencyKey(cmd.idempotencyKey()).orElseThrow(() -> race);
        }
        publisher.publish(saved, TransferSaga.start(saved));
        return saved;
    }

    public Optional<Transfer> find(String transferId) {
        return repository.findById(transferId);
    }

    public void onWithdrawEvent(WithdrawEvent event) {
        handle(event.transferId(), t -> TransferSaga.on(t, event));
    }

    public void onDepositEvent(DepositEvent event) {
        handle(event.transferId(), t -> TransferSaga.on(t, event));
    }

    public void applyTimeout(String transferId) {
        handle(transferId, TransferSaga::onTimeout);
    }

    private void handle(String transferId, Function<Transfer, SagaDecision> decide) {
        Optional<Transfer> maybe = repository.findById(transferId);
        if (maybe.isEmpty()) {
            log.warn("Event for unknown transferId {}", transferId);
            return;
        }

        Transfer t = maybe.get();
        SagaDecision decision = decide.apply(t);
        if (decision.changed()) {
            t.apply(decision, Instant.now());
            repository.update(t);
        }
        publisher.publish(t, decision);
    }
}
