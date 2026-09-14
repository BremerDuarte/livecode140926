package co.inter.piggies.coordinator.domain;

import co.inter.piggies.contracts.DepositCommand;
import co.inter.piggies.contracts.DepositEvent;
import co.inter.piggies.contracts.FailureReasons;
import co.inter.piggies.contracts.WithdrawCommand;
import co.inter.piggies.contracts.WithdrawEvent;
import java.util.List;

public final class TransferSaga {

    private TransferSaga() {}

    public static SagaDecision start(Transfer t) {
        return new SagaDecision(true, TransferStatus.PENDING, SagaStep.AWAITING_DEBIT_AUTHORIZATION, null,
                List.of(new WithdrawCommand.AuthorizeDebit(t.getId(), t.getFromAccount(), t.getAmountMinor())),
                List.of());
    }

    public static SagaDecision on(Transfer t, WithdrawEvent e) {
        return switch (e) {
            case WithdrawEvent.DebitAuthorized ignored -> switch (t.getStep()) {
                case AWAITING_DEBIT_AUTHORIZATION -> new SagaDecision(true,
                        TransferStatus.DEBIT_AUTHORIZED, SagaStep.AWAITING_CREDIT_AUTHORIZATION, null,
                        List.of(),
                        List.of(new DepositCommand.AuthorizeCredit(t.getId(), t.getToAccount(), t.getAmountMinor())));
                // late authorization for a transfer we already gave up on: release the hold
                case DONE -> isTimedOut(t)
                        ? new SagaDecision(false, t.getStatus(), t.getStep(), t.getFailureReason(),
                                List.of(new WithdrawCommand.CancelDebit(t.getId())), List.of())
                        : SagaDecision.ignore(t);
                default -> SagaDecision.ignore(t);
            };
            case WithdrawEvent.DebitRejected r -> switch (t.getStep()) {
                case AWAITING_DEBIT_AUTHORIZATION -> new SagaDecision(true,
                        TransferStatus.FAILED, SagaStep.DONE, r.reason(), List.of(), List.of());
                default -> SagaDecision.ignore(t);
            };
            case WithdrawEvent.DebitConfirmed ignored -> switch (t.getStep()) {
                case AWAITING_BOTH_CONFIRMATIONS -> new SagaDecision(true,
                        t.getStatus(), SagaStep.AWAITING_CREDIT_CONFIRMATION, t.getFailureReason(),
                        List.of(), List.of());
                case AWAITING_DEBIT_CONFIRMATION -> new SagaDecision(true,
                        TransferStatus.COMPLETED, SagaStep.DONE, t.getFailureReason(), List.of(), List.of());
                default -> SagaDecision.ignore(t);
            };
            case WithdrawEvent.DebitCancelled ignored -> switch (t.getStep()) {
                case AWAITING_DEBIT_CANCELLATION -> new SagaDecision(true,
                        FailureReasons.TIMEOUT.equals(t.getFailureReason())
                                ? TransferStatus.FAILED : TransferStatus.COMPENSATED,
                        SagaStep.DONE, t.getFailureReason(), List.of(), List.of());
                default -> SagaDecision.ignore(t);
            };
        };
    }

    public static SagaDecision on(Transfer t, DepositEvent e) {
        return switch (e) {
            case DepositEvent.CreditAuthorized ignored -> switch (t.getStep()) {
                case AWAITING_CREDIT_AUTHORIZATION -> new SagaDecision(true,
                        TransferStatus.CREDIT_AUTHORIZED, SagaStep.AWAITING_BOTH_CONFIRMATIONS, null,
                        List.of(new WithdrawCommand.ConfirmDebit(t.getId())),
                        List.of(new DepositCommand.ConfirmCredit(t.getId())));
                default -> SagaDecision.ignore(t);
            };
            case DepositEvent.CreditRejected r -> switch (t.getStep()) {
                case AWAITING_CREDIT_AUTHORIZATION -> new SagaDecision(true,
                        t.getStatus(), SagaStep.AWAITING_DEBIT_CANCELLATION, r.reason(),
                        List.of(new WithdrawCommand.CancelDebit(t.getId())), List.of());
                default -> SagaDecision.ignore(t);
            };
            case DepositEvent.CreditConfirmed ignored -> switch (t.getStep()) {
                case AWAITING_BOTH_CONFIRMATIONS -> new SagaDecision(true,
                        t.getStatus(), SagaStep.AWAITING_DEBIT_CONFIRMATION, t.getFailureReason(),
                        List.of(), List.of());
                case AWAITING_CREDIT_CONFIRMATION -> new SagaDecision(true,
                        TransferStatus.COMPLETED, SagaStep.DONE, t.getFailureReason(), List.of(), List.of());
                default -> SagaDecision.ignore(t);
            };
        };
    }

    public static SagaDecision onTimeout(Transfer t) {
        return switch (t.getStep()) {
            case AWAITING_DEBIT_AUTHORIZATION -> new SagaDecision(true,
                    TransferStatus.FAILED, SagaStep.DONE, FailureReasons.TIMEOUT, List.of(), List.of());
            case AWAITING_CREDIT_AUTHORIZATION -> new SagaDecision(true,
                    t.getStatus(), SagaStep.AWAITING_DEBIT_CANCELLATION, FailureReasons.TIMEOUT,
                    List.of(new WithdrawCommand.CancelDebit(t.getId())), List.of());
            default -> SagaDecision.ignore(t);
        };
    }

    private static boolean isTimedOut(Transfer t) {
        return FailureReasons.TIMEOUT.equals(t.getFailureReason());
    }
}
