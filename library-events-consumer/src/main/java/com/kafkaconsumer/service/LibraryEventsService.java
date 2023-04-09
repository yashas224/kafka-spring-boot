package com.kafkaconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaconsumer.entity.LibraryEvent;
import com.kafkaconsumer.repository.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

  @Autowired
  LibraryEventRepository libraryEventRepository;

  @Autowired
  ObjectMapper objectMapper;

  public void processLibraryEvents(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
    LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

    // for testing purpose
    if(libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId().equals(999)) {
      throw new RecoverableDataAccessException(" Database exception due to Network issue  ");
    }
    
    switch(libraryEvent.getLibraryEventType()) {
      case NEW:
        save(libraryEvent);
        break;
      case UPDATE:
        validate(libraryEvent);
        save(libraryEvent);
        break;
      default:
        log.error("Invalid Library Event type");
    }
  }

  private void validate(LibraryEvent libraryEvent) {
    // validation
    if(libraryEvent.getLibraryEventId() == null) {
      throw new IllegalArgumentException("library event Id is missing");
    }

    Optional<LibraryEvent> dbEntityOptional = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
    if(!dbEntityOptional.isPresent()) {
      throw new IllegalArgumentException("Not a valid  event Id is missing");
    }

    log.info(" Validation for LibraryEvent is successful : {}", dbEntityOptional.get());
  }

  private void save(LibraryEvent libraryEvent) {
    libraryEvent.getBook().setLibraryEvent(libraryEvent);

    libraryEvent = libraryEventRepository.save(libraryEvent);
    log.info("Saved Library Event : {}", libraryEvent);
  }
}
