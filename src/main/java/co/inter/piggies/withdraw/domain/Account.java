package co.inter.piggies.withdraw.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;


@Entity@Table(name = "withdraw_account")
@Getter@NoArgsConstructor@AllArgsConstructor

public class Account {
    @Id
    private String id;          // número da conta; é o que vem no comando
    private String country;
    private long balanceMinor;      // saldo total, inclui o que está retido
    private long heldMinor;         // parcela retida por holds AUTHORIZED
}