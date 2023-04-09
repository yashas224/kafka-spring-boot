package com.kafkaconsumer.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaconsumer.entity.FailureRecord;
import com.kafkaconsumer.repository.FailureRecordRepository;
import com.kafkaconsumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryScheduler {

  FailureRecordRepository failureRecordRepository;
  LibraryEventsService libraryEventsService;

  public RetryScheduler(FailureRecordRepository failureRecordRepository, LibraryEventsService libraryEventsService) {
    this.failureRecordRepository = failureRecordRepository;
    this.libraryEventsService = libraryEventsService;
  }

  @Scheduled(fixedRate = 10000)
  public void retryFailedRecords() {
    log.info("retryFailedRecords Scheduler started");
    failureRecordRepository.findByStatus("RETRY")
       .forEach(failureRecord -> {
         log.info("Retrying the failed record : {}", failureRecord);
         try {
           libraryEventsService.processLibraryEvents(buildConsumerRecord(failureRecord));
           failureRecord.setStatus("SUCCESS");
         } catch(Exception e) {
           log.error("Error in retryFailedRecords :{} ", e);
         }
       });

    log.info("retryFailedRecords Scheduler Completed");
  }

  private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
    return new ConsumerRecord<>(failureRecord.getTopic(), failureRecord.getPartition()
       , failureRecord.getOffsetValue(),
       failureRecord.getKey(), failureRecord.getErrorRecord());
  }
}
