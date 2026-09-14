package co.inter.piggies.contracts;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.micronaut.serde.annotation.Serdeable;

@Serdeable
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = DepositEvent.CreditAuthorized.class, name = "CreditAuthorized"),
    @JsonSubTypes.Type(value = DepositEvent.CreditRejected.class,   name = "CreditRejected"),
    @JsonSubTypes.Type(value = DepositEvent.CreditConfirmed.class,  name = "CreditConfirmed")
})
public sealed interface DepositEvent {
    String transferId();

    @Serdeable record CreditAuthorized(String transferId) implements DepositEvent {}
    @Serdeable record CreditRejected(String transferId, String reason) implements DepositEvent {}
    @Serdeable record CreditConfirmed(String transferId) implements DepositEvent {}
}
