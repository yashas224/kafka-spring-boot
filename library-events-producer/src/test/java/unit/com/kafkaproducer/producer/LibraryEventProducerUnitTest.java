package com.kafkaproducer.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaproducer.domain.Book;
import com.kafkaproducer.domain.LibraryEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = LibraryEventProducer.class)
public class LibraryEventProducerUnitTest {

  @SpyBean
  LibraryEventProducer libraryEventProducer;

  @MockBean
  KafkaTemplate<Integer, String> kafkaTemplate;

  @SpyBean
  ObjectMapper objectMapper;

  // argument captor  way
  @Test
  void testSendLibraryEventFailure() throws Exception {
    LibraryEvent libraryEvent = LibraryEvent.builder().book(Book.builder().bookId(1).bookName("LIFT").bookAuthor("yashas").build()).build();

    ArgumentCaptor<ListenableFutureCallback<SendResult>> callbackArgumentCaptor = ArgumentCaptor.forClass(ListenableFutureCallback.class);

    ListenableFuture future = Mockito.mock(ListenableFuture.class);
    Mockito.when(kafkaTemplate.sendDefault(Mockito.nullable(Integer.class), Mockito.anyString())).thenReturn(future);

    libraryEventProducer.sendLibraryEvent(libraryEvent);

    // callback captured
    Mockito.verify(future).addCallback(callbackArgumentCaptor.capture());

    ListenableFutureCallback<SendResult> listenableFutureCallback = callbackArgumentCaptor.getValue();
    listenableFutureCallback.onFailure(new KafkaException("Exception while sending message to kafka"));

    Mockito.verify(libraryEventProducer).sendLibraryEvent(Mockito.any(LibraryEvent.class));
    Mockito.verify(libraryEventProducer).handlefailure(Mockito.nullable(Integer.class), Mockito.anyString(), Mockito.any(KafkaException.class));

    Mockito.verify(kafkaTemplate).sendDefault(Mockito.isNull(), Mockito.anyString());
    Mockito.verifyNoMoreInteractions(kafkaTemplate);
  }

  // doAnswer()   way
  @Test
  void testSendLibraryEventSuccess() throws Exception {
    LibraryEvent libraryEvent = LibraryEvent.builder().book(Book.builder().bookId(1).bookName("LIFT").bookAuthor("yashas").build()).build();

    ListenableFuture future = Mockito.mock(ListenableFuture.class);
    Mockito.when(kafkaTemplate.sendDefault(Mockito.nullable(Integer.class), Mockito.anyString())).thenReturn(future);

    libraryEventProducer.sendLibraryEvent(libraryEvent);

    Mockito.doAnswer(invocation -> {
      ListenableFutureCallback<SendResult> listenableFutureCallback = invocation.getArgument(0);
      SendResult sendResult = Mockito.mock(SendResult.class);
      listenableFutureCallback.onSuccess(sendResult);
      Mockito.verify(libraryEventProducer).sendLibraryEvent(Mockito.any(LibraryEvent.class));
      Mockito.verify(libraryEventProducer).handleSuccess(Mockito.nullable(Integer.class), Mockito.anyString(), Mockito.eq(sendResult));

      return null;
    }).when(future).addCallback(Mockito.any(ListenableFutureCallback.class));

    Mockito.verify(kafkaTemplate).sendDefault(Mockito.isNull(), Mockito.anyString());
    Mockito.verifyNoMoreInteractions(kafkaTemplate);
  }
}
