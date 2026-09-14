package co.inter.piggies.coordinator.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import co.inter.piggies.coordinator.domain.SagaStep;
import co.inter.piggies.coordinator.infrastructure.persistence.TransferRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TimeoutSweeperTest {

    @Mock
    private TransferRepository repository;

    @Mock
    private TransferService service;

    private TimeoutSweeper sweeper;

    @BeforeEach
    void setUp() {
        sweeper = new TimeoutSweeper(repository, service, Duration.ofSeconds(5));
    }

    @Test
    void sweepTransitionsEveryExpiredTransfer() {
        when(repository.findExpired(any(SagaStep.class), any(SagaStep.class), any(Instant.class)))
                .thenReturn(List.of("t-1", "t-2"));

        sweeper.sweep(Instant.now().plusSeconds(60));

        verify(service, times(1)).applyTimeout("t-1");
        verify(service, times(1)).applyTimeout("t-2");
    }

    @Test
    void sweepDoesNothingWhenNoneAreExpired() {
        when(repository.findExpired(any(SagaStep.class), any(SagaStep.class), any(Instant.class)))
                .thenReturn(List.of());

        sweeper.sweep(Instant.now().minusSeconds(60));

        verify(service, never()).applyTimeout(any());
    }

    @Test
    void sweepAtAwaitingCreditAuthorizationYieldsExactlyOneApplyTimeout() {
        when(repository.findExpired(any(SagaStep.class), any(SagaStep.class), any(Instant.class)))
                .thenReturn(List.of("t-3"));

        sweeper.sweep(Instant.now());

        verify(service, times(1)).applyTimeout("t-3");
    }
}
