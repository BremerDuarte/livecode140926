package co.inter.piggies.coordinator.domain;

import static org.assertj.core.api.Assertions.assertThat;

import co.inter.piggies.contracts.DepositCommand;
import co.inter.piggies.contracts.DepositEvent;
import co.inter.piggies.contracts.FailureReasons;
import co.inter.piggies.contracts.WithdrawCommand;
import co.inter.piggies.contracts.WithdrawEvent;
import java.time.Instant;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TransferSagaTest {

    private static final String TRANSFER_ID = "t-1";
    private static final String FROM_ACCOUNT = "BR-1";
    private static final String FROM_COUNTRY = "BRA";
    private static final String TO_ACCOUNT = "US-1";
    private static final String TO_COUNTRY = "USA";
    private static final long AMOUNT_MINOR = 10_000L;

    private static Transfer transfer(SagaStep step) {
        return transfer(step, TransferStatus.PENDING, null);
    }

    private static Transfer transfer(SagaStep step, TransferStatus status, String failureReason) {
        Instant now = Instant.now();
        return new Transfer(TRANSFER_ID, FROM_ACCOUNT, FROM_COUNTRY, TO_ACCOUNT, TO_COUNTRY, AMOUNT_MINOR,
                status, step, failureReason, "idem-1", now, now);
    }

    @Nested
    class Start {

        @Test
        void emitsExactlyOneAuthorizeDebitAndNoDepositCommand() {
            Transfer t = Transfer.pending(TRANSFER_ID, FROM_ACCOUNT, FROM_COUNTRY, TO_ACCOUNT, TO_COUNTRY,
                    AMOUNT_MINOR, "idem-1", Instant.now());

            SagaDecision decision = TransferSaga.start(t);

            assertThat(decision.changed()).isTrue();
            assertThat(decision.status()).isEqualTo(TransferStatus.PENDING);
            assertThat(decision.step()).isEqualTo(SagaStep.AWAITING_DEBIT_AUTHORIZATION);
            assertThat(decision.depositCommands()).isEmpty();
            assertThat(decision.withdrawCommands()).containsExactly(
                    new WithdrawCommand.AuthorizeDebit(TRANSFER_ID, FROM_ACCOUNT, AMOUNT_MINOR));
        }
    }

    @Nested
    class DebitAuthorization {

        @Test
        void debitAuthorizedMovesToAwaitingCreditAuthorizationAndEmitsAuthorizeCredit() {
            Transfer t = transfer(SagaStep.AWAITING_DEBIT_AUTHORIZATION);

            SagaDecision decision = TransferSaga.on(t, new WithdrawEvent.DebitAuthorized(TRANSFER_ID));

            assertThat(decision.changed()).isTrue();
            assertThat(decision.status()).isEqualTo(TransferStatus.DEBIT_AUTHORIZED);
            assertThat(decision.step()).isEqualTo(SagaStep.AWAITING_CREDIT_AUTHORIZATION);
            assertThat(decision.withdrawCommands()).isEmpty();
            assertThat(decision.depositCommands()).containsExactly(
                    new DepositCommand.AuthorizeCredit(TRANSFER_ID, TO_ACCOUNT, AMOUNT_MINOR));
        }

        @Test
        void debitRejectedFailsWithoutContactingDeposit() {
            Transfer t = transfer(SagaStep.AWAITING_DEBIT_AUTHORIZATION);

            SagaDecision decision = TransferSaga.on(t,
                    new WithdrawEvent.DebitRejected(TRANSFER_ID, FailureReasons.INSUFFICIENT_FUNDS));

            assertThat(decision.changed()).isTrue();
            assertThat(decision.status()).isEqualTo(TransferStatus.FAILED);
            assertThat(decision.step()).isEqualTo(SagaStep.DONE);
            assertThat(decision.failureReason()).isEqualTo(FailureReasons.INSUFFICIENT_FUNDS);
            assertThat(decision.withdrawCommands()).isEmpty();
            assertThat(decision.depositCommands()).isEmpty();
        }
    }

    @Nested
    class CreditAuthorization {

        @Test
        void creditAuthorizedEmitsBothConfirmations() {
            Transfer t = transfer(SagaStep.AWAITING_CREDIT_AUTHORIZATION, TransferStatus.DEBIT_AUTHORIZED, null);

            SagaDecision decision = TransferSaga.on(t, new DepositEvent.CreditAuthorized(TRANSFER_ID));

            assertThat(decision.changed()).isTrue();
            assertThat(decision.status()).isEqualTo(TransferStatus.CREDIT_AUTHORIZED);
            assertThat(decision.step()).isEqualTo(SagaStep.AWAITING_BOTH_CONFIRMATIONS);
            assertThat(decision.withdrawCommands()).containsExactly(new WithdrawCommand.ConfirmDebit(TRANSFER_ID));
            assertThat(decision.depositCommands()).containsExactly(new DepositCommand.ConfirmCredit(TRANSFER_ID));
        }

        @Test
        void creditRejectedCancelsTheDebitAndRecordsTheReason() {
            Transfer t = transfer(SagaStep.AWAITING_CREDIT_AUTHORIZATION, TransferStatus.DEBIT_AUTHORIZED, null);

            SagaDecision decision = TransferSaga.on(t,
                    new DepositEvent.CreditRejected(TRANSFER_ID, FailureReasons.ACCOUNT_NOT_FOUND));

            assertThat(decision.changed()).isTrue();
            assertThat(decision.step()).isEqualTo(SagaStep.AWAITING_DEBIT_CANCELLATION);
            assertThat(decision.failureReason()).isEqualTo(FailureReasons.ACCOUNT_NOT_FOUND);
            assertThat(decision.withdrawCommands()).containsExactly(new WithdrawCommand.CancelDebit(TRANSFER_ID));
            assertThat(decision.depositCommands()).isEmpty();
        }
    }

    @Nested
    class Confirmations {

        @Test
        void debitThenCreditCompletes() {
            Transfer t = transfer(SagaStep.AWAITING_BOTH_CONFIRMATIONS, TransferStatus.CREDIT_AUTHORIZED, null);

            SagaDecision afterDebit = TransferSaga.on(t, new WithdrawEvent.DebitConfirmed(TRANSFER_ID));
            assertThat(afterDebit.step()).isEqualTo(SagaStep.AWAITING_CREDIT_CONFIRMATION);
            assertThat(afterDebit.status()).isEqualTo(TransferStatus.CREDIT_AUTHORIZED);
            t.apply(afterDebit, Instant.now());

            SagaDecision afterCredit = TransferSaga.on(t, new DepositEvent.CreditConfirmed(TRANSFER_ID));
            assertThat(afterCredit.status()).isEqualTo(TransferStatus.COMPLETED);
            assertThat(afterCredit.step()).isEqualTo(SagaStep.DONE);
        }

        @Test
        void creditThenDebitCompletes() {
            Transfer t = transfer(SagaStep.AWAITING_BOTH_CONFIRMATIONS, TransferStatus.CREDIT_AUTHORIZED, null);

            SagaDecision afterCredit = TransferSaga.on(t, new DepositEvent.CreditConfirmed(TRANSFER_ID));
            assertThat(afterCredit.step()).isEqualTo(SagaStep.AWAITING_DEBIT_CONFIRMATION);
            assertThat(afterCredit.status()).isEqualTo(TransferStatus.CREDIT_AUTHORIZED);
            t.apply(afterCredit, Instant.now());

            SagaDecision afterDebit = TransferSaga.on(t, new WithdrawEvent.DebitConfirmed(TRANSFER_ID));
            assertThat(afterDebit.status()).isEqualTo(TransferStatus.COMPLETED);
            assertThat(afterDebit.step()).isEqualTo(SagaStep.DONE);
        }

        @Test
        void neitherConfirmationAloneCompletes() {
            Transfer awaitingBoth = transfer(SagaStep.AWAITING_BOTH_CONFIRMATIONS, TransferStatus.CREDIT_AUTHORIZED, null);
            assertThat(TransferSaga.on(awaitingBoth, new WithdrawEvent.DebitConfirmed(TRANSFER_ID)).status())
                    .isNotEqualTo(TransferStatus.COMPLETED);
            assertThat(TransferSaga.on(awaitingBoth, new DepositEvent.CreditConfirmed(TRANSFER_ID)).status())
                    .isNotEqualTo(TransferStatus.COMPLETED);
        }
    }

    @Nested
    class Compensation {

        @Test
        void debitCancelledCompensatesAndPreservesTheFailureReason() {
            Transfer t = transfer(SagaStep.AWAITING_DEBIT_CANCELLATION, TransferStatus.DEBIT_AUTHORIZED,
                    FailureReasons.ACCOUNT_NOT_FOUND);

            SagaDecision decision = TransferSaga.on(t, new WithdrawEvent.DebitCancelled(TRANSFER_ID));

            assertThat(decision.changed()).isTrue();
            assertThat(decision.status()).isEqualTo(TransferStatus.COMPENSATED);
            assertThat(decision.step()).isEqualTo(SagaStep.DONE);
            assertThat(decision.failureReason()).isEqualTo(FailureReasons.ACCOUNT_NOT_FOUND);
        }
    }

    @Nested
    class Timeout {

        @Test
        void atAwaitingDebitAuthorizationFailsWithNoCommands() {
            Transfer t = transfer(SagaStep.AWAITING_DEBIT_AUTHORIZATION);

            SagaDecision decision = TransferSaga.onTimeout(t);

            assertThat(decision.changed()).isTrue();
            assertThat(decision.status()).isEqualTo(TransferStatus.FAILED);
            assertThat(decision.step()).isEqualTo(SagaStep.DONE);
            assertThat(decision.failureReason()).isEqualTo(FailureReasons.TIMEOUT);
            assertThat(decision.withdrawCommands()).isEmpty();
            assertThat(decision.depositCommands()).isEmpty();
        }

        @Test
        void atAwaitingCreditAuthorizationCancelsTheDebit() {
            Transfer t = transfer(SagaStep.AWAITING_CREDIT_AUTHORIZATION, TransferStatus.DEBIT_AUTHORIZED, null);

            SagaDecision decision = TransferSaga.onTimeout(t);

            assertThat(decision.changed()).isTrue();
            assertThat(decision.step()).isEqualTo(SagaStep.AWAITING_DEBIT_CANCELLATION);
            assertThat(decision.failureReason()).isEqualTo(FailureReasons.TIMEOUT);
            assertThat(decision.withdrawCommands()).containsExactly(new WithdrawCommand.CancelDebit(TRANSFER_ID));
        }

        @ParameterizedTest
        @EnumSource(value = SagaStep.class, names = {
                "AWAITING_BOTH_CONFIRMATIONS", "AWAITING_DEBIT_CONFIRMATION", "AWAITING_CREDIT_CONFIRMATION"})
        void neverFiresDuringTheConfirmationPhase(SagaStep step) {
            Transfer t = transfer(step, TransferStatus.CREDIT_AUTHORIZED, null);

            SagaDecision decision = TransferSaga.onTimeout(t);

            assertThat(decision.changed()).isFalse();
            assertThat(decision.withdrawCommands()).isEmpty();
            assertThat(decision.depositCommands()).isEmpty();
        }

        @Test
        void ignoredAtDone() {
            Transfer t = transfer(SagaStep.DONE, TransferStatus.COMPLETED, null);

            SagaDecision decision = TransferSaga.onTimeout(t);

            assertThat(decision.changed()).isFalse();
        }
    }

    @Nested
    class LateEvents {

        @Test
        void debitAuthorizedAfterATimeoutFailureStillCancelsTheDebit() {
            Transfer t = transfer(SagaStep.DONE, TransferStatus.FAILED, FailureReasons.TIMEOUT);

            SagaDecision decision = TransferSaga.on(t, new WithdrawEvent.DebitAuthorized(TRANSFER_ID));

            assertThat(decision.changed()).isFalse();
            assertThat(decision.status()).isEqualTo(TransferStatus.FAILED);
            assertThat(decision.withdrawCommands()).containsExactly(new WithdrawCommand.CancelDebit(TRANSFER_ID));
        }

        @Test
        void debitAuthorizedAfterANonTimeoutFailureIsIgnored() {
            Transfer t = transfer(SagaStep.DONE, TransferStatus.FAILED, FailureReasons.INSUFFICIENT_FUNDS);

            SagaDecision decision = TransferSaga.on(t, new WithdrawEvent.DebitAuthorized(TRANSFER_ID));

            assertThat(decision.changed()).isFalse();
            assertThat(decision.withdrawCommands()).isEmpty();
        }
    }

    @Nested
    class Redelivery {

        @Test
        void duplicateDebitAuthorizedEmitsOnlyOneAuthorizeCredit() {
            Transfer t = transfer(SagaStep.AWAITING_DEBIT_AUTHORIZATION);
            SagaDecision first = TransferSaga.on(t, new WithdrawEvent.DebitAuthorized(TRANSFER_ID));
            t.apply(first, Instant.now());

            SagaDecision second = TransferSaga.on(t, new WithdrawEvent.DebitAuthorized(TRANSFER_ID));

            assertThat(second.changed()).isFalse();
            assertThat(second.depositCommands()).isEmpty();
        }

        @Test
        void duplicateCreditConfirmedAfterCompletedChangesNothing() {
            Transfer t = transfer(SagaStep.DONE, TransferStatus.COMPLETED, null);

            SagaDecision decision = TransferSaga.on(t, new DepositEvent.CreditConfirmed(TRANSFER_ID));

            assertThat(decision.changed()).isFalse();
            assertThat(decision.status()).isEqualTo(TransferStatus.COMPLETED);
        }
    }

    @Nested
    class Totality {

        @ParameterizedTest
        @EnumSource(SagaStep.class)
        void withdrawEventsNeverThrowAndNeverMoveADoneTransfer(SagaStep step) {
            for (WithdrawEvent event : allWithdrawEvents()) {
                Transfer t = transfer(step, TransferStatus.PENDING, null);
                SagaDecision decision = TransferSaga.on(t, event);
                if (step == SagaStep.DONE) {
                    assertThat(decision.step()).isEqualTo(SagaStep.DONE);
                }
            }
        }

        @ParameterizedTest
        @EnumSource(SagaStep.class)
        void depositEventsNeverThrowAndNeverMoveADoneTransfer(SagaStep step) {
            for (DepositEvent event : allDepositEvents()) {
                Transfer t = transfer(step, TransferStatus.PENDING, null);
                SagaDecision decision = TransferSaga.on(t, event);
                if (step == SagaStep.DONE) {
                    assertThat(decision.step()).isEqualTo(SagaStep.DONE);
                }
            }
        }

        @ParameterizedTest
        @EnumSource(SagaStep.class)
        void timeoutNeverThrowsAndNeverMovesADoneTransfer(SagaStep step) {
            Transfer t = transfer(step, TransferStatus.PENDING, null);
            SagaDecision decision = TransferSaga.onTimeout(t);
            if (step == SagaStep.DONE) {
                assertThat(decision.step()).isEqualTo(SagaStep.DONE);
            }
        }

        private static java.util.List<WithdrawEvent> allWithdrawEvents() {
            return java.util.List.of(
                    new WithdrawEvent.DebitAuthorized(TRANSFER_ID),
                    new WithdrawEvent.DebitRejected(TRANSFER_ID, FailureReasons.INSUFFICIENT_FUNDS),
                    new WithdrawEvent.DebitConfirmed(TRANSFER_ID),
                    new WithdrawEvent.DebitCancelled(TRANSFER_ID));
        }

        private static java.util.List<DepositEvent> allDepositEvents() {
            return java.util.List.of(
                    new DepositEvent.CreditAuthorized(TRANSFER_ID),
                    new DepositEvent.CreditRejected(TRANSFER_ID, FailureReasons.ACCOUNT_NOT_FOUND),
                    new DepositEvent.CreditConfirmed(TRANSFER_ID));
        }
    }
}
