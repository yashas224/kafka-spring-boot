package com.kafkaconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaconsumer.entity.Book;
import com.kafkaconsumer.entity.LibraryEvent;
import com.kafkaconsumer.entity.LibraryEventType;
import com.kafkaconsumer.repository.FailureRecordRepository;
import com.kafkaconsumer.repository.LibraryEventRepository;
import com.kafkaconsumer.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events", "library-events.RETRY", "library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
   "spring.kafka.template.default-topic=library-events",
   "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
   "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.IntegerSerializer",
   "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
   "retry-consumer-startup=false",
   "recovery-type=db-recovery",
   "manual-ack=true"
})
public class LibraryEventsConsumerMannualOffsetCommitIntgTest {

  private static final Logger logger = Logger.getLogger(LibraryEventsConsumerMannualOffsetCommitIntgTest.class.getName());

  @Value("${recovery-type}")
  private String recoveryType;

  @Autowired
  EmbeddedKafkaBroker embeddedKafkaBroker;

  @Value("${spring.embedded.kafka.brokers}")
  private String brokerAddresses;

  @Autowired
  KafkaTemplate<Integer, String> kafkaTemplate;

  @Autowired
  KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @Autowired
  ObjectMapper objectMapper;

  @SpyBean
  LibraryEventsService libraryEventsService;

  @SpyBean
  LibraryEventsConsumerMannualOffsetCommit libraryEventsConsumerMannualOffsetCommit;

  @Autowired
  LibraryEventRepository libraryEventRepository;

  Consumer<Integer, String> consumer;

  @Autowired
  FailureRecordRepository failureRecordRepository;

  @Value("${topics.retry}")
  private String retryTopic;

  @Value("${topics.dlt}")
  private String dltTopic;

  @BeforeEach
  public void setup() {
    // start all consumers
//    for(MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
//      ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
//    }

    // integration tests for LibraryEventsConsumerMannualOffsetCommit.java consumer with default group ID library-events-consumer-group
    // so only start that consumer
    var consumerContainer = kafkaListenerEndpointRegistry.getListenerContainers().stream().
       filter(messageListenerContainer -> Objects.equals(messageListenerContainer.getGroupId(), "library-events-consumer-group"))
       .collect(Collectors.toList()).get(0);

    ContainerTestUtils.waitForAssignment(consumerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
  }

  @AfterEach
  public void tearDown() {
    libraryEventRepository.deleteAll();
    failureRecordRepository.deleteAll();
  }

  @Test
  void publishNewLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
    logger.info("Embedded broker address".concat(brokerAddresses));
    // given publish message
    LibraryEvent publishedLibraryEvent = LibraryEvent.builder().libraryEventType(LibraryEventType.NEW)
       .book(Book.builder().bookId(456).bookName("testing Kafka Consumer Application ").bookAuthor("Yashas Samaga").build()).build();

    kafkaTemplate.sendDefault(objectMapper.writeValueAsString(publishedLibraryEvent)).get();

    CountDownLatch latch = new CountDownLatch(1);
    latch.await(2, TimeUnit.SECONDS);

    // verification

    Mockito.verify(libraryEventsConsumerMannualOffsetCommit).onMessage(Mockito.any(ConsumerRecord.class), Mockito.any(Acknowledgment.class));
    Mockito.verify(libraryEventsService).processLibraryEvents(Mockito.any(ConsumerRecord.class));

    // database check

    List<LibraryEvent> savedLibraryEvents = libraryEventRepository.findAll();
    Assertions.assertEquals(1, savedLibraryEvents.size());
    savedLibraryEvents.stream().forEach(savedEntity -> {
      // assertions to prove that the id is set by the database
      Assertions.assertNotNull(savedEntity.getLibraryEventId());
      Assertions.assertEquals(publishedLibraryEvent.getBook().getBookName(), savedEntity.getBook().getBookName());
      Assertions.assertEquals(publishedLibraryEvent.getBook().getBookAuthor(), savedEntity.getBook().getBookAuthor());
      Assertions.assertEquals(publishedLibraryEvent.getBook().getBookId(), savedEntity.getBook().getBookId());
    });
  }

  @Test
  public void publishUpdateLibraryEvent() throws JsonProcessingException, InterruptedException, ExecutionException {
    // given db entry already present
    LibraryEvent dbEntry = LibraryEvent.builder().libraryEventType(LibraryEventType.NEW)
       .book(Book.builder().bookId(456).bookName("testing Kafka Consumer Application ").bookAuthor("Yashas Samaga").build()).build();
    dbEntry.getBook().setLibraryEvent(dbEntry);

    libraryEventRepository.save(dbEntry);

    // publish update Event
    Book updatedBook = Book.builder().bookId(456).bookName("testing Kafka Consumer Application v3").bookAuthor("Krishnamoorthy").build();
    dbEntry.setBook(updatedBook);
    dbEntry.setLibraryEventType(LibraryEventType.UPDATE);

    String publishedLibraryEvent = objectMapper.writeValueAsString(dbEntry);
    logger.info("published Update Library Event :".concat(publishedLibraryEvent));

    kafkaTemplate.sendDefault(publishedLibraryEvent).get();

    CountDownLatch latch = new CountDownLatch(1);
    latch.await(2, TimeUnit.SECONDS);

    // verification

    Mockito.verify(libraryEventsConsumerMannualOffsetCommit).onMessage(Mockito.any(ConsumerRecord.class), Mockito.any(Acknowledgment.class));
    Mockito.verify(libraryEventsService).processLibraryEvents(Mockito.any(ConsumerRecord.class));

    LibraryEvent persistedLibraryEvent = libraryEventRepository.findById(dbEntry.getLibraryEventId()).get();
    Assertions.assertEquals(updatedBook.getBookName(), persistedLibraryEvent.getBook().getBookName());
  }

  private void assumeProperty(String recoveryType, String requriedRecoveryType) {
    Assumptions.assumeTrue(requriedRecoveryType.equalsIgnoreCase(recoveryType));
  }

  // when DeadLetterPublishingRecoverer in com.kafkaconsumer.config.LibraryEventsConsumerConfig.publishingRecoverer is used
  // recovery-type = topic-recovery

  @Test
  public void publishUpdateLibraryEvent_null_libraryEvent() throws JsonProcessingException, InterruptedException, ExecutionException {
    assumeProperty(recoveryType, "topic-recovery");
    // publish update Event
    LibraryEvent libraryEvent = new LibraryEvent();
    Book book = Book.builder().bookId(456).bookName("testing Kafka Consumer Application v3").bookAuthor("Krishnamoorthy").build();
    libraryEvent.setBook(book);
    libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);

    String publishedLibraryEvent = objectMapper.writeValueAsString(libraryEvent);
    logger.info("published Update Library Event :".concat(publishedLibraryEvent));

    kafkaTemplate.sendDefault(publishedLibraryEvent).get();

    CountDownLatch latch = new CountDownLatch(1);
    latch.await(5, TimeUnit.SECONDS);

    // verification

    Mockito.verify(libraryEventsConsumerMannualOffsetCommit).onMessage(Mockito.any(ConsumerRecord.class), Mockito.any(Acknowledgment.class));
    Mockito.verify(libraryEventsService).processLibraryEvents(Mockito.any(ConsumerRecord.class));

    // setup consumer
    setupConsumeForDLT();
    //
    ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, dltTopic);
    Assertions.assertEquals(publishedLibraryEvent, record.value());
    logger.info("Message received in topic :".concat(record.topic()).concat(" is : ").concat(record.value()));
  }

  public void setupConsumeForDLT() {
    Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group2", "true", embeddedKafkaBroker));
    consumer = new DefaultKafkaConsumerFactory(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
    embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, dltTopic);
  }

  // when DeadLetterPublishingRecoverer in com.kafkaconsumer.config.LibraryEventsConsumerConfig.publishingRecoverer is used
  // recovery-type = topic-recovery

  @Test
  public void publishUpdateLibraryEvent_null_libraryEventId_999() throws JsonProcessingException, InterruptedException, ExecutionException {
    assumeProperty(recoveryType, "topic-recovery");

    // publish update Event
    LibraryEvent libraryEvent = new LibraryEvent();
    Book book = Book.builder().bookId(456).bookName("testing Kafka Consumer Application v3").bookAuthor("Krishnamoorthy").build();
    libraryEvent.setBook(book);
    libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
    libraryEvent.setLibraryEventId(999);

    String publishedLibraryEvent = objectMapper.writeValueAsString(libraryEvent);
    logger.info("published Update Library Event :".concat(publishedLibraryEvent));

    kafkaTemplate.sendDefault(publishedLibraryEvent).get();

    CountDownLatch latch = new CountDownLatch(1);
    latch.await(5, TimeUnit.SECONDS);

    // verification

    Mockito.verify(libraryEventsConsumerMannualOffsetCommit, Mockito.times(3)).onMessage(Mockito.any(ConsumerRecord.class), Mockito.any(Acknowledgment.class));
    Mockito.verify(libraryEventsService, Mockito.times(3)).processLibraryEvents(Mockito.any(ConsumerRecord.class));

    // setup consumer
    setupConsumer();
    //
    ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, retryTopic);
    Assertions.assertEquals(publishedLibraryEvent, record.value());
    logger.info("Message received in topic :".concat(record.topic()).concat(" is : ").concat(record.value()));
  }

  public void setupConsumer() {
    Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
    consumer = new DefaultKafkaConsumerFactory(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
    embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);
  }

  // when ConsumerRecordRecoverer in com.kafkaconsumer.config.LibraryEventsConsumerConfig.consumerRecordRecoverer is used
  // recovery-type = db-recovery
  @Test
  public void publishUpdateLibraryEvent_null_libraryEventForConsumerRecordRecoverer() throws JsonProcessingException, InterruptedException, ExecutionException {
    assumeProperty(recoveryType, "db-recovery");

    // publish update Event
    LibraryEvent libraryEvent = new LibraryEvent();
    Book book = Book.builder().bookId(456).bookName("testing Kafka Consumer Application v3").bookAuthor("Krishnamoorthy").build();
    libraryEvent.setBook(book);
    libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);

    String publishedLibraryEvent = objectMapper.writeValueAsString(libraryEvent);
    logger.info("published Update Library Event :".concat(publishedLibraryEvent));

    kafkaTemplate.sendDefault(publishedLibraryEvent).get();

    CountDownLatch latch = new CountDownLatch(1);
    latch.await(5, TimeUnit.SECONDS);

    // verification

    Mockito.verify(libraryEventsConsumerMannualOffsetCommit).onMessage(Mockito.any(ConsumerRecord.class), Mockito.any(Acknowledgment.class));
    Mockito.verify(libraryEventsService).processLibraryEvents(Mockito.any(ConsumerRecord.class));

    var failureRecordList = failureRecordRepository.findAll();
    Assertions.assertEquals(1, failureRecordList.size());
    logger.info("Failed Record in the Database ".concat(failureRecordList.get(0).toString()));
    Assertions.assertEquals(publishedLibraryEvent, failureRecordList.get(0).getErrorRecord());
    Assertions.assertEquals("DEAD", failureRecordList.get(0).getStatus());
  }
}
