package co.inter.piggies.withdraw;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record ConfirmDebit(String transferId) implements WithdrawCommand {}