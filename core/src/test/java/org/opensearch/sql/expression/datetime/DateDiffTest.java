/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.Temporal;
import java.util.TimeZone;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DateDiffTest extends DateTimeTestBase {

  // Function signature is:
  // (DATE/DATETIME/TIMESTAMP/TIME, DATE/DATETIME/TIMESTAMP/TIME) -> LONG
  private static Stream<Arguments> getTestData() {
    return Stream.of(
        Arguments.of(LocalTime.of(12, 42), LocalTime.of(7, 40), 0L),
        Arguments.of(LocalTime.of(12, 42), LocalDate.now(), 0L),
        Arguments.of(LocalTime.of(12, 42), LocalDateTime.now(), 0L),
        Arguments.of(LocalTime.of(12, 42),
            Instant.now().plusMillis(TimeZone.getDefault().getRawOffset()), 0L),
        Arguments.of(LocalDate.of(2022, 6, 6), LocalTime.of(12, 42),
            -DAYS.between(LocalDate.of(2022, 6, 6), LocalDate.now())),
        Arguments.of(LocalDate.of(2022, 6, 6), LocalDate.of(1999, 12, 31),
            -DAYS.between(LocalDate.of(2022, 6, 6), LocalDate.of(1999, 12, 31))),
        Arguments.of(LocalDate.of(2022, 6, 6), LocalDateTime.of(1961, 4, 12, 9, 7),
            -DAYS.between(LocalDate.of(2022, 6, 6), LocalDate.of(1961, 4, 12))),
        Arguments.of(LocalDate.of(2022, 6, 6), Instant.ofEpochSecond(42),
            -DAYS.between(LocalDate.of(2022, 6, 6), LocalDate.of(1970, 1, 1))),
        Arguments.of(LocalDateTime.of(1961, 4, 12, 9, 7), LocalTime.now(),
            -DAYS.between(LocalDate.of(1961, 4, 12), LocalDate.now())),
        Arguments.of(LocalDateTime.of(1961, 4, 12, 9, 7), LocalDate.of(1993, 3, 4),
            -DAYS.between(LocalDate.of(1961, 4, 12), LocalDate.of(1993, 3, 4))),
        Arguments.of(LocalDateTime.of(1961, 4, 12, 9, 7), LocalDateTime.of(1993, 3, 4, 5, 6),
            -DAYS.between(LocalDate.of(1961, 4, 12), LocalDate.of(1993, 3, 4))),
        Arguments.of(LocalDateTime.of(1961, 4, 12, 9, 7), Instant.ofEpochSecond(0),
            -DAYS.between(LocalDate.of(1961, 4, 12), LocalDate.of(1970, 1, 1))),
        Arguments.of(Instant.ofEpochSecond(0), LocalTime.MAX,
            -DAYS.between(LocalDate.of(1970, 1, 1), LocalDate.now())),
        Arguments.of(Instant.ofEpochSecond(0), LocalDate.of(1993, 3, 4),
            -DAYS.between(LocalDate.of(1970, 1, 1), LocalDate.of(1993, 3, 4))),
        Arguments.of(Instant.ofEpochSecond(0), LocalDateTime.of(1993, 3, 4, 5, 6),
            -DAYS.between(LocalDate.of(1970, 1, 1), LocalDate.of(1993, 3, 4))),
        Arguments.of(Instant.ofEpochSecond(0), Instant.now(),
            -DAYS.between(LocalDate.of(1970, 1, 1), LocalDateTime.now(ZoneId.of("UTC"))))
      );
  }

  @ParameterizedTest
  @MethodSource("getTestData")
  public void try_different_data(Temporal arg1, Temporal arg2, Long expectedResult) {
    assertEquals(expectedResult, datediff(arg1, arg2));
  }
}
