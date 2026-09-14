package co.inter.piggies.withdraw;


import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record AuthorizeDebit(String transferId, String account, long amountMinor) implements WithdrawCommand {
}