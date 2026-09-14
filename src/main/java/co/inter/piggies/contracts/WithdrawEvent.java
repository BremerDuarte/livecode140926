package co.inter.piggies.contracts;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.micronaut.serde.annotation.Serdeable;

@Serdeable
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = WithdrawEvent.DebitAuthorized.class, name = "DebitAuthorized"),
    @JsonSubTypes.Type(value = WithdrawEvent.DebitRejected.class,   name = "DebitRejected"),
    @JsonSubTypes.Type(value = WithdrawEvent.DebitConfirmed.class,  name = "DebitConfirmed"),
    @JsonSubTypes.Type(value = WithdrawEvent.DebitCancelled.class,  name = "DebitCancelled")
})
public sealed interface WithdrawEvent {
    String transferId();

    @Serdeable record DebitAuthorized(String transferId) implements WithdrawEvent {}
    @Serdeable record DebitRejected(String transferId, String reason) implements WithdrawEvent {}
    @Serdeable record DebitConfirmed(String transferId) implements WithdrawEvent {}
    @Serdeable record DebitCancelled(String transferId) implements WithdrawEvent {}
}
