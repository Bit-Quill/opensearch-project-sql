/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.data.model;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * Expression Time Value.
 */
@RequiredArgsConstructor
public class ExprTimeValue extends AbstractExprValue {
  private final LocalTime time;

  private static final DateTimeFormatter FORMATTER_VARIABLE_NANOS;
  private static final DateTimeFormatter FORMATTER_ZERO_PAD_NANOS;
  private static final int MIN_FRACTION_SECONDS = 0;
  private static final int MAX_FRACTION_SECONDS = 9;
  private static final int FRACTION_SECONDS_TO_PRINT = 6;

  static {
    FORMATTER_VARIABLE_NANOS = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(
                    ChronoField.NANO_OF_SECOND,
                    MIN_FRACTION_SECONDS,
                    MAX_FRACTION_SECONDS,
                    true)
            .toFormatter();

    FORMATTER_ZERO_PAD_NANOS = new DateTimeFormatterBuilder()
        .appendPattern("HH:mm:ss")
        .appendFraction(
            ChronoField.NANO_OF_SECOND,
            FRACTION_SECONDS_TO_PRINT,
            FRACTION_SECONDS_TO_PRINT,
            true)
        .toFormatter();
  }

  /**
   * Constructor.
   */
  public ExprTimeValue(String time) {
    try {
      this.time = LocalTime.parse(time, FORMATTER_VARIABLE_NANOS);
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(String.format("time:%s in unsupported format, please use "
          + "HH:mm:ss[.SSSSSSSSS]", time));
    }
  }

  @Override
  public String value() {
    return time.format(FORMATTER_ZERO_PAD_NANOS);
  }

  @Override
  public ExprType type() {
    return ExprCoreType.TIME;
  }

  @Override
  public LocalTime timeValue() {
    return time;
  }

  @Override
  public String toString() {
    return String.format("TIME '%s'", value());
  }

  @Override
  public int compare(ExprValue other) {
    return time.compareTo(other.timeValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return time.equals(other.timeValue());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(time);
  }
}
