package com.kafkaconsumer.entity;

import lombok.*;

import javax.persistence.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class Book {
  @Id
  private Integer bookId;
  private String bookName;
  private String bookAuthor;

  @OneToOne(cascade = CascadeType.ALL)
  @ToString.Exclude
  private LibraryEvent libraryEvent;
}
