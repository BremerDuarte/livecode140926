package co.inter.piggies.coordinator.application;

import co.inter.piggies.coordinator.domain.SagaStep;
import co.inter.piggies.coordinator.infrastructure.persistence.TransferRepository;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.time.Instant;

@Singleton
public class TimeoutSweeper {

    private final TransferRepository repository;
    private final TransferService service;
    private final Duration sagaTimeout;

    public TimeoutSweeper(TransferRepository repository, TransferService service,
            @Value("${piggies.coordinator.saga-timeout:5s}") Duration sagaTimeout) {
        this.repository = repository;
        this.service = service;
        this.sagaTimeout = sagaTimeout;
    }

    @Scheduled(fixedDelay = "${piggies.coordinator.sweep-interval:1s}")
    void sweepNow() {
        sweep(Instant.now().minus(sagaTimeout));
    }

    public void sweep(Instant deadline) {
        for (String id : repository.findExpired(SagaStep.AWAITING_DEBIT_AUTHORIZATION,
                SagaStep.AWAITING_CREDIT_AUTHORIZATION, deadline)) {
            service.applyTimeout(id);
        }
    }
}
