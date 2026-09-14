package co.inter.piggies.contracts;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.micronaut.serde.annotation.Serdeable;

@Serdeable
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = DepositCommand.AuthorizeCredit.class, name = "AuthorizeCredit"),
    @JsonSubTypes.Type(value = DepositCommand.ConfirmCredit.class,   name = "ConfirmCredit")
})
public sealed interface DepositCommand {
    String transferId();

    @Serdeable record AuthorizeCredit(String transferId, String account, long amountMinor) implements DepositCommand {}
    @Serdeable record ConfirmCredit(String transferId) implements DepositCommand {}
}
