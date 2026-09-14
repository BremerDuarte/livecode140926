package co.inter.piggies.withdraw;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.micronaut.serde.annotation.Serdeable;

@Serdeable
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AuthorizeDebit.class, name = "AuthorizeDebit"),
        @JsonSubTypes.Type(value = ConfirmDebit.class,   name = "ConfirmDebit"),
        @JsonSubTypes.Type(value = CancelDebit.class,    name = "CancelDebit")
})
public sealed interface WithdrawCommand permits AuthorizeDebit, ConfirmDebit, CancelDebit {
    String transferId();
}