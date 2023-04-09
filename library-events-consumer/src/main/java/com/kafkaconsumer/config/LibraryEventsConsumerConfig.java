package com.kafkaconsumer.config;

import com.kafkaconsumer.entity.FailureRecord;
import com.kafkaconsumer.service.FailureRecordService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

  @Autowired
  KafkaTemplate<Integer, String> kafkaTemplate;

  @Value("${topics.retry}")
  private String retryTopic;

  @Value("${topics.dlt}")
  private String dltTopic;

  @Autowired
  FailureRecordService failureRecordService;

  @Value("${recovery-type:db-recovery}")
  private String recoveryType;

  @Bean
  ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
     ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
     ConsumerFactory<Object, Object> kafkaConsumerFactory) {
    ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
    configurer.configure(factory, kafkaConsumerFactory);
    factory.setConcurrency(3);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    factory.setCommonErrorHandler(errorHandler());
    return factory;
  }

  public DefaultErrorHandler errorHandler() {
    BackOff fixedBackOff = new FixedBackOff(1000l, 2);

    var expontialBackoff = new ExponentialBackOffWithMaxRetries(2);
    expontialBackoff.setInitialInterval(1000l);
    expontialBackoff.setMultiplier(2);

    DefaultErrorHandler errorHandler;

    if(recoveryType.equals("topic-recovery")) {
      errorHandler = new DefaultErrorHandler(publishingRecoverer(), expontialBackoff);
    } else {
      // new error handler with custom   Recoverer
      errorHandler = new DefaultErrorHandler(consumerRecordRecoverer, expontialBackoff);
    }

    //listners
    errorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> {
      log.info("Failed Record in retry listner, Exception : {} , delivery attempt : {} ", ex.getMessage(), deliveryAttempt);
    }));

    errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
    return errorHandler;
  }

  // publishing to different topics based on the type of exception after the retires have exhausted

  public DeadLetterPublishingRecoverer publishingRecoverer() {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
       (r, e) -> {
         log.info("Inside  publishingRecoverer");
         if(e.getCause() instanceof RecoverableDataAccessException) {
           log.info("Inside  RecoverableDataAccessException block");
           return new TopicPartition(retryTopic, r.partition());
         } else {
           return new TopicPartition(dltTopic, r.partition());
         }
       });

    return recoverer;
  }

  // custom  Recoverer

  ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {
    log.info("Inside  custom consumerRecordRecoverer");
    var record = (ConsumerRecord<Integer, String>) consumerRecord;
    if(e.getCause() instanceof RecoverableDataAccessException) {
      // recovery logic
      failureRecordService.saveFailedrecords(record, e, "RETRY");
    } else {
      // non recovery logic
      failureRecordService.saveFailedrecords(record, e, "DEAD");
    }
  };
}
