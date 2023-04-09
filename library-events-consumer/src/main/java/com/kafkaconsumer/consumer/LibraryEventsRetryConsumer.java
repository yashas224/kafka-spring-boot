package com.kafkaconsumer.consumer;

import com.kafkaconsumer.service.LibraryEventsService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsRetryConsumer {

  @Autowired
  LibraryEventsService libraryEventsService;

  @SneakyThrows
  @KafkaListener(topics = "${topics.retry}",
     groupId = "retry-consumer-group",
     autoStartup = "${retry-consumer-startup:true}"
  )
  public void onMessage(ConsumerRecord<Integer, String> data) {
    log.info("Consumer Record in topic {}  : {} ", data.topic(), data);
    data.headers().forEach(header -> {
      log.info("\n");
      log.info("Header key : {} - Header Value : {}", header.key(), new String(header.value()));
    });
    libraryEventsService.processLibraryEvents(data);
  }
}
