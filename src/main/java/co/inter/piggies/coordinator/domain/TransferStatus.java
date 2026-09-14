package co.inter.piggies.coordinator.domain;

public enum TransferStatus {
    PENDING,
    DEBIT_AUTHORIZED,
    CREDIT_AUTHORIZED,
    COMPLETED,
    FAILED,
    COMPENSATED
}
