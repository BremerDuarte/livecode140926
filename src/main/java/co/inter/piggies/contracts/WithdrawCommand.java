package co.inter.piggies.contracts;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.micronaut.serde.annotation.Serdeable;

@Serdeable
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = WithdrawCommand.AuthorizeDebit.class, name = "AuthorizeDebit"),
    @JsonSubTypes.Type(value = WithdrawCommand.ConfirmDebit.class,   name = "ConfirmDebit"),
    @JsonSubTypes.Type(value = WithdrawCommand.CancelDebit.class,    name = "CancelDebit")
})
public sealed interface WithdrawCommand {
    String transferId();

    @Serdeable record AuthorizeDebit(String transferId, String account, long amountMinor) implements WithdrawCommand {}
    @Serdeable record ConfirmDebit(String transferId) implements WithdrawCommand {}
    @Serdeable record CancelDebit(String transferId) implements WithdrawCommand {}
}
