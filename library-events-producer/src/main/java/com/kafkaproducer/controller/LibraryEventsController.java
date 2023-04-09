package com.kafkaproducer.controller;

import com.kafkaproducer.domain.LibraryEvent;
import com.kafkaproducer.domain.LibraryEventType;
import com.kafkaproducer.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@RestController
@RequestMapping("")
@Slf4j
public class LibraryEventsController {

  @Autowired
  LibraryEventProducer libraryEventProducer;

  @PostMapping(value = "/v1/libraryevent", consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {
    libraryEvent.setLibraryEventType(LibraryEventType.NEW);
    libraryEventProducer.sendLibraryEvent(libraryEvent);
    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }

  @PostMapping("/v1/libraryevent-header")
  public ResponseEntity<LibraryEvent> postLibraryEvent1(@RequestBody LibraryEvent libraryEvent) {
    libraryEvent.setLibraryEventType(LibraryEventType.NEW);
    libraryEventProducer.sendLibraryEventWithHeader(libraryEvent);
    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }

  @PostMapping("/v1/libraryevent-sync")
  public ResponseEntity<LibraryEvent> postLibraryEventSync(@RequestBody LibraryEvent libraryEvent) throws Exception {
    libraryEvent.setLibraryEventType(LibraryEventType.NEW);
    SendResult result = libraryEventProducer.sendLibraryEventSync(libraryEvent);
    log.info("Result  is : {}", result.toString());
    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }

  @PutMapping(value = "/v1/libraryevent", consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {
    if(libraryEvent.getLibraryEventId() == null) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Pass the libraryEventId");
    }
    libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
    libraryEventProducer.sendLibraryEvent(libraryEvent);
    return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
  }
}
