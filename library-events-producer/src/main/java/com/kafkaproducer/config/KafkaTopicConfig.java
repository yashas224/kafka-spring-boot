package com.kafkaproducer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("local")
public class KafkaTopicConfig {

  @Bean
  public NewTopic libraryEventsTopic() {
    return new NewTopic("library-events", 3, (short) 3);
  }
}