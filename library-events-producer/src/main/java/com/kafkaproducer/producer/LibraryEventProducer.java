package com.kafkaproducer.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class LibraryEventProducer {
  @Autowired
  KafkaTemplate<Integer, String> kafkaTemplate;

  @Autowired
  ObjectMapper objectMapper;

  public void sendLibraryEvent(LibraryEvent event) {
    log.info("From sendLibraryEvent");

    try {
      Integer key = event.getLibraryEventId();
      String message = objectMapper.writeValueAsString(event);

      ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture = kafkaTemplate.sendDefault(key, message);
      sendResultListenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
        @Override
        public void onFailure(Throwable ex) {
          handlefailure(key, message, ex);
        }

        @Override
        public void onSuccess(SendResult<Integer, String> result) {
          handleSuccess(key, message, result);
        }
      });
    } catch(Exception e) {
      log.error("Error in  sendLibraryEvent !!!");
    }
  }

  public void sendLibraryEventWithHeader(LibraryEvent event) {
    log.info("From sendLibraryEventWithHeader");
    try {
      Integer key = event.getLibraryEventId();
      String message = objectMapper.writeValueAsString(event);

      List<Header> headerList = Arrays.asList(new RecordHeader("event-source", "scanner".getBytes()));
      ProducerRecord producerRecord = new ProducerRecord("library-events", null, key, message, headerList);

      ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture = kafkaTemplate.send(producerRecord);
      sendResultListenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
        @Override
        public void onFailure(Throwable ex) {
          handlefailure(key, message, ex);
        }

        @Override
        public void onSuccess(SendResult<Integer, String> result) {
          handleSuccess(key, message, result);
        }
      });
    } catch(Exception e) {
      log.error("Error in  sendLibraryEventWithHeader !!!");
    }
  }

  public SendResult sendLibraryEventSync(LibraryEvent event) throws Exception {
    log.info("From sendLibraryEventSync");

    SendResult result = null;
    try {
      Integer key = event.getLibraryEventId();
      String message = objectMapper.writeValueAsString(event);
      result = kafkaTemplate.sendDefault(key, message).get(1, TimeUnit.SECONDS);
    } catch(Exception e) {
      log.error("Error in  sendLibraryEventSync !!!");
      throw new RuntimeException("Error in  sendLibraryEventSync !!!");
    }
    return result;
  }

  public void handlefailure(Integer key, String message, Throwable ex) {
    log.error("Error sending message !!! exception is {}", ex);
  }

  public void handleSuccess(Integer key, String message, SendResult<Integer, String> result) {
    log.info("Message Sent Successfully for \n key : {} \n  value is : {} \n partition : {}", key, message, result.getRecordMetadata().partition());
  }
}
