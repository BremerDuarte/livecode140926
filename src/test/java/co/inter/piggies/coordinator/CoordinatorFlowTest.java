package co.inter.piggies.coordinator;

import static org.assertj.core.api.Assertions.assertThat;

import co.inter.piggies.contracts.DepositCommand;
import co.inter.piggies.contracts.DepositEvent;
import co.inter.piggies.contracts.FailureReasons;
import co.inter.piggies.contracts.Topics;
import co.inter.piggies.contracts.WithdrawCommand;
import co.inter.piggies.contracts.WithdrawEvent;
import co.inter.piggies.coordinator.domain.TransferStatus;
import co.inter.piggies.coordinator.infrastructure.http.TransferController;
import co.inter.piggies.support.AbstractContainerTest;
import co.inter.piggies.support.TestContainers;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class CoordinatorFlowTest extends AbstractContainerTest {

    private static final String CONSUMER_ENABLED = "piggies.test.consumer.enabled";

    @Inject
    @Client("/")
    HttpClient httpClient;

    @Inject
    EventProducer eventProducer;

    @Inject
    CommandSpy commandSpy;

    /**
     * Micronaut calls this before the context starts, the only window in which the topics can
     * be created ahead of the listeners subscribing to them.
     */
    @Override
    public Map<String, String> getProperties() {
        TestContainers.createTopic(Topics.withdrawCmd("BRA"), 1);
        TestContainers.createTopic(Topics.depositCmd("USA"), 1);
        TestContainers.createTopic(Topics.WITHDRAW_EVT, 1);
        TestContainers.createTopic(Topics.DEPOSIT_EVT, 1);

        Map<String, String> properties = new HashMap<>(super.getProperties());
        // long window: the 5s production timeout would race the sweeper against these
        // event-driven assertions and flake.
        properties.put("piggies.coordinator.saga-timeout", "60s");
        properties.put(CONSUMER_ENABLED, "true");
        return properties;
    }

    @Test
    void postAcceptsAndReturns202Pending() {
        var response = httpClient.toBlocking().exchange(
                HttpRequest.POST("/transfers", submitBody("demo-1")), TransferController.SubmitResponse.class);

        assertThat(response.code()).isEqualTo(HttpStatus.ACCEPTED.getCode());
        assertThat(response.body().status()).isEqualTo(TransferStatus.PENDING);
    }

    @Test
    void authorizeDebitIsObservedOnTheWithdrawCommandTopic() {
        TransferController.SubmitResponse response = submit("demo-2");

        Awaitility.await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(commandSpy.withdrawCommands())
                        .anyMatch(c -> c instanceof WithdrawCommand.AuthorizeDebit
                                && c.transferId().equals(response.transferId())));
    }

    @Test
    void happyPathWalksToCompleted() {
        String id = submit("demo-happy").transferId();

        eventProducer.withdrawEvent(id, new WithdrawEvent.DebitAuthorized(id));
        eventProducer.depositEvent(id, new DepositEvent.CreditAuthorized(id));
        eventProducer.withdrawEvent(id, new WithdrawEvent.DebitConfirmed(id));
        eventProducer.depositEvent(id, new DepositEvent.CreditConfirmed(id));

        Awaitility.await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(get(id).status()).isEqualTo(TransferStatus.COMPLETED));
    }

    @Test
    void debitRejectedFailsWithoutContactingDeposit() {
        String id = submit("demo-rejected").transferId();

        eventProducer.withdrawEvent(id,
                new WithdrawEvent.DebitRejected(id, FailureReasons.INSUFFICIENT_FUNDS));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            TransferController.TransferView view = get(id);
            assertThat(view.status()).isEqualTo(TransferStatus.FAILED);
            assertThat(view.failureReason()).isEqualTo(FailureReasons.INSUFFICIENT_FUNDS);
        });

        assertThat(commandSpy.depositCommands()).noneMatch(c -> c.transferId().equals(id));
    }

    @Test
    void repeatedSubmitWithSameIdempotencyKeyReturnsTheSameTransferId() {
        String first = submit("demo-idem").transferId();
        String second = submit("demo-idem").transferId();

        assertThat(second).isEqualTo(first);
    }

    private TransferController.SubmitResponse submit(String idempotencyKey) {
        return httpClient.toBlocking().retrieve(
                HttpRequest.POST("/transfers", submitBody(idempotencyKey)), TransferController.SubmitResponse.class);
    }

    private TransferController.TransferView get(String transferId) {
        return httpClient.toBlocking().retrieve(
                HttpRequest.GET("/transfers/" + transferId), TransferController.TransferView.class);
    }

    private TransferController.SubmitRequest submitBody(String idempotencyKey) {
        return new TransferController.SubmitRequest("BR-1", "BRA", "US-1", "USA", 10_000L, idempotencyKey);
    }

    @KafkaClient
    interface EventProducer {
        @Topic(Topics.WITHDRAW_EVT)
        void withdrawEvent(@KafkaKey String transferId, WithdrawEvent event);

        @Topic(Topics.DEPOSIT_EVT)
        void depositEvent(@KafkaKey String transferId, DepositEvent event);
    }

    /**
     * Stands in for two {@code kafka-console-consumer} terminals watching the command topics.
     */
    @Requires(property = CONSUMER_ENABLED, value = "true")
    @KafkaListener(groupId = "coordinator-flow-test", offsetReset = OffsetReset.EARLIEST)
    static class CommandSpy {

        private final Queue<WithdrawCommand> withdrawCommands = new ConcurrentLinkedQueue<>();
        private final Queue<DepositCommand> depositCommands = new ConcurrentLinkedQueue<>();

        @Topic(Topics.WITHDRAW_CMD_PREFIX + "BRA")
        void onWithdrawCommand(WithdrawCommand command) {
            withdrawCommands.add(command);
        }

        @Topic(Topics.DEPOSIT_CMD_PREFIX + "USA")
        void onDepositCommand(DepositCommand command) {
            depositCommands.add(command);
        }

        Queue<WithdrawCommand> withdrawCommands() {
            return withdrawCommands;
        }

        Queue<DepositCommand> depositCommands() {
            return depositCommands;
        }
    }
}
