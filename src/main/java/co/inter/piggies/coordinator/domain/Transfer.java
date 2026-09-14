package co.inter.piggies.coordinator.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import lombok.Getter;
import org.jspecify.annotations.Nullable;

@Entity
@Table(name = "transfers")
@Getter
public class Transfer {

    @Id
    private String id;

    private String fromAccount;
    private String fromCountry;
    private String toAccount;
    private String toCountry;
    private long amountMinor;

    @Enumerated(EnumType.STRING)
    private TransferStatus status;

    @Enumerated(EnumType.STRING)
    private SagaStep step;

    @Nullable
    private String failureReason;

    @Column(unique = true, nullable = false, updatable = false)
    private String idempotencyKey;

    private Instant createdAt;
    private Instant updatedAt;

    @Version
    private long version;

    protected Transfer() {
        // JPA
    }

    Transfer(String id, String fromAccount, String fromCountry, String toAccount, String toCountry,
            long amountMinor, TransferStatus status, SagaStep step, @Nullable String failureReason,
            String idempotencyKey, Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.fromAccount = fromAccount;
        this.fromCountry = fromCountry;
        this.toAccount = toAccount;
        this.toCountry = toCountry;
        this.amountMinor = amountMinor;
        this.status = status;
        this.step = step;
        this.failureReason = failureReason;
        this.idempotencyKey = idempotencyKey;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public static Transfer pending(String id, String fromAccount, String fromCountry, String toAccount,
            String toCountry, long amountMinor, String idempotencyKey, Instant now) {
        return new Transfer(id, fromAccount, fromCountry, toAccount, toCountry, amountMinor,
                TransferStatus.PENDING, SagaStep.AWAITING_DEBIT_AUTHORIZATION, null, idempotencyKey, now, now);
    }

    public void apply(SagaDecision decision, Instant now) {
        this.status = decision.status();
        this.step = decision.step();
        this.failureReason = decision.failureReason();
        this.updatedAt = now;
    }
}
