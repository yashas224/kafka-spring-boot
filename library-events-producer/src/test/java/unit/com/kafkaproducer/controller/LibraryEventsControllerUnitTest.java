package com.kafkaproducer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaproducer.domain.Book;
import com.kafkaproducer.domain.LibraryEvent;
import com.kafkaproducer.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(controllers = LibraryEventsController.class)
public class LibraryEventsControllerUnitTest {

  @Autowired
  MockMvc mockMvc;

  @Autowired
  ObjectMapper objectMapper;

  @MockBean
  LibraryEventProducer libraryEventProducer;

  @Test
  void testPostLibraryEvent() throws Exception {
    LibraryEvent libraryEvent = LibraryEvent.builder().book(Book.builder().bookId(1).bookName("LIFT").bookAuthor("yashas").build()).build();

    Mockito.doNothing().when(libraryEventProducer).sendLibraryEvent(Mockito.eq(libraryEvent));
    mockMvc.perform(post("/v1/libraryevent")
          .content(objectMapper.writeValueAsString(libraryEvent))
          .contentType(MediaType.APPLICATION_JSON))
       .andExpect(status().isCreated());
  }

  @Test
  void testPostLibraryEvent_400() throws Exception {
    LibraryEvent libraryEvent = LibraryEvent.builder().book(Book.builder().bookId(null).bookName(null).bookAuthor("yashas").build()).build();

    Mockito.doNothing().when(libraryEventProducer).sendLibraryEvent(Mockito.eq(libraryEvent));
    mockMvc.perform(post("/v1/libraryevent")
          .content(objectMapper.writeValueAsString(libraryEvent))
          .contentType(MediaType.APPLICATION_JSON))
       .andExpect(status().isBadRequest())
       .andExpect(content().string("{\"errors\":[\"book.bookId : must not be null\",\"book.bookName : must not be null\"]}"));
  }
}
