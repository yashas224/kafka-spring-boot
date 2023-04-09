package com.kafkaproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaproducer.domain.Book;
import com.kafkaproducer.domain.LibraryEvent;
import com.kafkaproducer.domain.LibraryEventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.logging.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
   "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsControllerIntegrationTest {

  private static final Logger LOGGER = Logger.getLogger(LibraryEventsControllerIntegrationTest.class.getName());
  @Autowired
  TestRestTemplate testRestTemplate;

  Consumer<Integer, String> consumer;

  @Autowired
  EmbeddedKafkaBroker embeddedKafkaBroker;

  @Autowired
  ObjectMapper objectMapper;

  @Value("${spring.embedded.kafka.brokers}")
  private String embeddedServerAddress;

  @BeforeEach
  public void setup() {
    Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("test-case-consumer-group", "true", embeddedKafkaBroker));
    consumer = new DefaultKafkaConsumerFactory(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
    embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "library-events");
  }

  @AfterEach
  public void tearDown() {
    consumer.close();
  }

  @Test
  void postLibraryEvent() throws JsonProcessingException {
    LOGGER.info("Embedded Server addredd : ".concat(embeddedServerAddress));
    LibraryEvent libraryEvent = LibraryEvent.builder().book(Book.builder().bookId(1).bookName("LIFT").bookAuthor("yashas").build()).build();
    HttpHeaders headers = new HttpHeaders();
    headers.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
    HttpEntity entity = new HttpEntity(libraryEvent, headers);

    ResponseEntity<LibraryEvent> response = testRestTemplate.exchange("/v1/libraryevent", HttpMethod.POST, entity, LibraryEvent.class);

    Assertions.assertNotNull(response);
    Assertions.assertEquals(HttpStatus.CREATED, response.getStatusCode());
    LibraryEvent responseBody = response.getBody();
    Assertions.assertEquals("LIFT", responseBody.getBook().getBookName());

    LibraryEvent ExpectedResponseObject = LibraryEvent.builder().book(Book.builder().bookId(1).bookName("LIFT").bookAuthor("yashas").build()).libraryEventType(LibraryEventType.NEW).build();

    ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
    String value = consumerRecord.value();
    Assertions.assertEquals(objectMapper.writeValueAsString(ExpectedResponseObject), value);
    Assertions.assertNull(consumerRecord.key());
  }
}
