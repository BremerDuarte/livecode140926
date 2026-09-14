package co.inter.piggies.withdraw;


import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record CancelDebit(String transferId) implements WithdrawCommand {}