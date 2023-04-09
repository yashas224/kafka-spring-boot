package com.kafkaconsumer.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class LibraryEvent {
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private Integer libraryEventId;

  @Enumerated(EnumType.STRING)
  private LibraryEventType libraryEventType;

  @OneToOne(mappedBy = "libraryEvent", cascade = CascadeType.ALL)
  private Book book;
}
