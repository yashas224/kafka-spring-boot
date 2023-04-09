package com.kafkaconsumer.service;

import com.kafkaconsumer.entity.FailureRecord;
import com.kafkaconsumer.repository.FailureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureRecordService {

  FailureRecordRepository failureRecordRepository;

  public FailureRecordService(FailureRecordRepository failureRecordRepository) {
    this.failureRecordRepository = failureRecordRepository;
  }

  public void saveFailedrecords(ConsumerRecord<Integer, String> consumerRecord, Exception e, String status) {
    FailureRecord failureRecord = FailureRecord.builder()
       .status(status)
       .topic(consumerRecord.topic())
       .key(consumerRecord.key())
       .errorRecord(consumerRecord.value())
       .partition(consumerRecord.partition())
       .offsetValue(consumerRecord.offset())
       .exception(e.getCause().getMessage())
       .build();

    failureRecordRepository.save(failureRecord);

    log.info("Saved Entity : {}", failureRecord);
  }
}
