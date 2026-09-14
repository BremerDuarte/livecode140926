package co.inter.piggies.coordinator.domain;

import co.inter.piggies.contracts.DepositCommand;
import co.inter.piggies.contracts.WithdrawCommand;
import java.util.List;
import org.jspecify.annotations.Nullable;

public record SagaDecision(boolean changed, TransferStatus status, SagaStep step,
                           @Nullable String failureReason,
                           List<WithdrawCommand> withdrawCommands,
                           List<DepositCommand> depositCommands) {

    public static SagaDecision ignore(Transfer t) {
        return new SagaDecision(false, t.getStatus(), t.getStep(), t.getFailureReason(), List.of(), List.of());
    }
}
