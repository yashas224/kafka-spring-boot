package com.kafkaconsumer.consumer;

import com.kafkaconsumer.service.LibraryEventsService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@ConditionalOnProperty(name = "manual-ack", havingValue = "true")
public class LibraryEventsConsumerMannualOffsetCommit implements AcknowledgingMessageListener<Integer, String> {

  @Autowired
  LibraryEventsService libraryEventsService;

  @SneakyThrows
  @Override
  @KafkaListener(topics = {"library-events"})
  public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
    log.info("Consumer Record that would be acknowledged manually : {} ", data);
    libraryEventsService.processLibraryEvents(data);
    acknowledgment.acknowledge();
  }
}
