package co.inter.piggies.coordinator.infrastructure.persistence;

import co.inter.piggies.coordinator.domain.SagaStep;
import co.inter.piggies.coordinator.domain.Transfer;
import io.micronaut.data.annotation.Query;
import io.micronaut.data.annotation.Repository;
import io.micronaut.data.repository.CrudRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface TransferRepository extends CrudRepository<Transfer, String> {

    Optional<Transfer> findByIdempotencyKey(String idempotencyKey);

    @Query("select t.id from Transfer t where t.step in (:a, :b) and t.updatedAt < :deadline")
    List<String> findExpired(SagaStep a, SagaStep b, Instant deadline);
}
