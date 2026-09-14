package co.inter.piggies.coordinator.infrastructure.messaging;

import co.inter.piggies.contracts.DepositEvent;
import co.inter.piggies.contracts.Topics;
import co.inter.piggies.contracts.WithdrawEvent;
import co.inter.piggies.coordinator.application.TransferService;
import io.micronaut.configuration.kafka.annotation.ConsumerCreationStrategy;
import io.micronaut.configuration.kafka.annotation.ErrorStrategy;
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaListener(groupId = "piggies-coordinator",
               offsetReset = OffsetReset.EARLIEST,
               threads = 1,
               consumerCreationStrategy = ConsumerCreationStrategy.PER_CLASS,
               errorStrategy = @ErrorStrategy(value = ErrorStrategyValue.RETRY_ON_ERROR,
                                              retryCount = 5, retryDelay = "100ms"))
public class SagaEventListener {

    private final TransferService service;

    public SagaEventListener(TransferService service) {
        this.service = service;
    }

    @Topic(Topics.WITHDRAW_EVT)
    void onWithdraw(WithdrawEvent e) {
        service.onWithdrawEvent(e);
    }

    @Topic(Topics.DEPOSIT_EVT)
    void onDeposit(DepositEvent e) {
        service.onDepositEvent(e);
    }
}
