package com.kafkaconsumer.repository;

import com.kafkaconsumer.entity.FailureRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface FailureRecordRepository extends JpaRepository<FailureRecord, Integer> {

  List<FailureRecord> findByStatus(String status);
}
