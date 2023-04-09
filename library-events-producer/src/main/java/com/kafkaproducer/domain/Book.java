package com.kafkaproducer.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {
  @NotNull
  private Integer bookId;
  @NotNull
  private String bookName;
  @NotNull
  private String bookAuthor;
}
