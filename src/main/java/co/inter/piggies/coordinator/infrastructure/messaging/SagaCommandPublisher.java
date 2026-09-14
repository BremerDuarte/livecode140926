package co.inter.piggies.coordinator.infrastructure.messaging;

import co.inter.piggies.contracts.DepositCommand;
import co.inter.piggies.contracts.Topics;
import co.inter.piggies.contracts.WithdrawCommand;
import co.inter.piggies.coordinator.domain.SagaDecision;
import co.inter.piggies.coordinator.domain.Transfer;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import jakarta.inject.Singleton;

@Singleton
public class SagaCommandPublisher {

    private final Client client;

    public SagaCommandPublisher(Client client) {
        this.client = client;
    }

    public void publish(Transfer t, SagaDecision d) {
        for (WithdrawCommand c : d.withdrawCommands()) {
            client.send(Topics.withdrawCmd(t.getFromCountry()), t.getId(), c);
        }
        for (DepositCommand c : d.depositCommands()) {
            client.send(Topics.depositCmd(t.getToCountry()), t.getId(), c);
        }
    }

    @KafkaClient(id = "coordinator-commands")
    interface Client {
        void send(@Topic String topic, @KafkaKey String transferId, WithdrawCommand command);
        void send(@Topic String topic, @KafkaKey String transferId, DepositCommand command);
    }
}
