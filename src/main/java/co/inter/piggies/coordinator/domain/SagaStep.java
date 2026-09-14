package co.inter.piggies.coordinator.domain;

public enum SagaStep {
    AWAITING_DEBIT_AUTHORIZATION,
    AWAITING_CREDIT_AUTHORIZATION,
    AWAITING_BOTH_CONFIRMATIONS,
    AWAITING_DEBIT_CONFIRMATION,    // credit already confirmed
    AWAITING_CREDIT_CONFIRMATION,   // debit already confirmed
    AWAITING_DEBIT_CANCELLATION,
    DONE
}
