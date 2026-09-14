package co.inter.piggies.coordinator.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import co.inter.piggies.contracts.WithdrawEvent;
import co.inter.piggies.coordinator.domain.SagaDecision;
import co.inter.piggies.coordinator.domain.SagaStep;
import co.inter.piggies.coordinator.domain.Transfer;
import co.inter.piggies.coordinator.domain.TransferStatus;
import co.inter.piggies.coordinator.infrastructure.messaging.SagaCommandPublisher;
import co.inter.piggies.coordinator.infrastructure.persistence.TransferRepository;
import jakarta.persistence.PersistenceException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransferServiceTest {

    @Mock
    private TransferRepository repository;

    @Mock
    private SagaCommandPublisher publisher;

    private TransferService service;

    @BeforeEach
    void setUp() {
        service = new TransferService(repository, publisher);
    }

    private static TransferService.SubmitCommand aCommand() {
        return new TransferService.SubmitCommand("BR-1", "BRA", "US-1", "USA", 10_000L, "idem-1");
    }

    @Test
    void secondSubmitWithTheSameIdempotencyKeyReturnsTheSameTransferAndPublishesNothing() {
        Instant now = Instant.now();
        Transfer existing = Transfer.pending("t-1", "BR-1", "BRA", "US-1", "USA", 10_000L, "idem-1", now);
        when(repository.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(existing));

        Transfer result = service.submit(aCommand());

        assertThat(result.getId()).isEqualTo("t-1");
        verify(publisher, never()).publish(any(), any());
    }

    @Test
    void aUniqueConstraintRaceReReadsAndReturnsTheWinner() {
        Instant now = Instant.now();
        Transfer winner = Transfer.pending("winner-id", "BR-1", "BRA", "US-1", "USA", 10_000L, "idem-1", now);
        when(repository.findByIdempotencyKey("idem-1"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(winner));
        when(repository.save(any())).thenThrow(new PersistenceException("unique violation"));

        Transfer result = service.submit(aCommand());

        assertThat(result.getId()).isEqualTo("winner-id");
        verify(publisher, never()).publish(any(), any());
    }

    @Test
    void submitPersistsAndPublishesForANewTransfer() {
        when(repository.findByIdempotencyKey("idem-1")).thenReturn(Optional.empty());
        when(repository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        Transfer result = service.submit(aCommand());

        assertThat(result.getStatus()).isEqualTo(TransferStatus.PENDING);
        assertThat(result.getStep()).isEqualTo(SagaStep.AWAITING_DEBIT_AUTHORIZATION);
        verify(publisher, times(1)).publish(any(), any());
    }

    @Test
    void eventForAnUnknownTransferIdNeitherThrowsNorPublishes() {
        when(repository.findById("missing")).thenReturn(Optional.empty());

        service.onWithdrawEvent(new WithdrawEvent.DebitConfirmed("missing"));

        verify(publisher, never()).publish(any(), any());
        verify(repository, never()).update(any());
    }

    @Test
    void aDecisionThatDidNotChangeNeitherUpdatesNorSkipsPublishingCommands() {
        Instant now = Instant.now();
        Transfer done = Transfer.pending("t-1", "BR-1", "BRA", "US-1", "USA", 10_000L, "idem-1", now);
        done.apply(new SagaDecision(true, TransferStatus.COMPLETED, SagaStep.DONE, null, List.of(), List.of()), now);
        when(repository.findById("t-1")).thenReturn(Optional.of(done));

        service.onWithdrawEvent(new WithdrawEvent.DebitConfirmed("t-1"));

        verify(repository, never()).update(any());
        verify(publisher, times(1)).publish(any(), any());
    }
}
