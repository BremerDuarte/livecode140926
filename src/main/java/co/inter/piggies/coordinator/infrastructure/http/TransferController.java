package co.inter.piggies.coordinator.infrastructure.http;

import co.inter.piggies.coordinator.application.TransferService;
import co.inter.piggies.coordinator.domain.SagaStep;
import co.inter.piggies.coordinator.domain.Transfer;
import co.inter.piggies.coordinator.domain.TransferStatus;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

@Controller("/transfers")
@ExecuteOn(TaskExecutors.BLOCKING)
public class TransferController {

    private final TransferService service;

    public TransferController(TransferService service) {
        this.service = service;
    }

    @Post
    @Status(HttpStatus.ACCEPTED)
    public SubmitResponse submit(@Valid @Body SubmitRequest request) {
        Transfer t = service.submit(new TransferService.SubmitCommand(
                request.fromAccount(), request.fromCountry(),
                request.toAccount(), request.toCountry(),
                request.amountMinor(), request.idempotencyKey()));
        return new SubmitResponse(t.getId(), t.getStatus());
    }

    @Get("/{transferId}")
    public HttpResponse<TransferView> get(String transferId) {
        return service.find(transferId)
                .<HttpResponse<TransferView>>map(t -> HttpResponse.ok(TransferView.from(t)))
                .orElseGet(HttpResponse::notFound);
    }

    @Serdeable
    public record SubmitRequest(
            @NotBlank String fromAccount,
            @NotBlank String fromCountry,
            @NotBlank String toAccount,
            @NotBlank String toCountry,
            @Positive long amountMinor,
            @NotBlank String idempotencyKey) {}

    @Serdeable
    public record SubmitResponse(String transferId, TransferStatus status) {}

    @Serdeable
    public record TransferView(String transferId, TransferStatus status, SagaStep step,
            @Nullable String failureReason, Instant updatedAt) {

        static TransferView from(Transfer t) {
            return new TransferView(t.getId(), t.getStatus(), t.getStep(), t.getFailureReason(), t.getUpdatedAt());
        }
    }
}
