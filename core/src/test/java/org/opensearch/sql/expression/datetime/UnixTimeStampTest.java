/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;

public class UnixTimeStampTest extends ExpressionTestBase {

  Environment<Expression, ExprValue> env;

  private FunctionExpression unixTimeStampExpr() {
    var repo = new ExpressionConfig().functionRepository();
    var func = repo.resolve(new FunctionSignature(new FunctionName("unix_timestamp"), List.of()));
    return (FunctionExpression)func.apply(List.of());
  }

  private Long unixTimeStamp() {
    return unixTimeStampExpr().valueOf(null).longValue();
  }

  private FunctionExpression unixTimeStampOf(Expression value) {
    var repo = new ExpressionConfig().functionRepository();
    var func = repo.resolve(new FunctionSignature(new FunctionName("unix_timestamp"),
        List.of(value.type())));
    return (FunctionExpression)func.apply(List.of(value));
  }

  private Double unixTimeStampOf(Double value) {
    return unixTimeStampOf(DSL.literal(value)).valueOf(null).doubleValue();
  }

  private Double unixTimeStampOf(LocalDate value) {
    return unixTimeStampOf(DSL.literal(new ExprDateValue(value))).valueOf(null).doubleValue();
  }

  private Double unixTimeStampOf(LocalDateTime value) {
    return unixTimeStampOf(DSL.literal(new ExprDatetimeValue(value))).valueOf(null).doubleValue();
  }

  private Double unixTimeStampOf(Instant value) {
    return unixTimeStampOf(DSL.literal(new ExprTimestampValue(value))).valueOf(null).doubleValue();
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }

  @Test
  public void checkNoArgs() {
    assertEquals(System.currentTimeMillis() / 1000L, unixTimeStamp());
    assertEquals(System.currentTimeMillis() / 1000L, eval(unixTimeStampExpr()).longValue());
  }

  private static Stream<Arguments> getDateSamples() {
    return Stream.of(
        Arguments.of(LocalDate.of(1984, 1, 1)),
        Arguments.of(LocalDate.of(2000, 2, 29)),
        Arguments.of(LocalDate.of(1999, 12, 31)),
        Arguments.of(LocalDate.of(2004, 2, 29)),
        Arguments.of(LocalDate.of(2100, 2, 28)),
        Arguments.of(LocalDate.of(2012, 2, 21))
    );
  }

  @ParameterizedTest
  @MethodSource("getDateSamples")
  public void checkOfDate(LocalDate date) {
    assertEquals(date.getLong(ChronoField.EPOCH_DAY) * 24 * 3600, unixTimeStampOf(date));
    assertEquals(date.getLong(ChronoField.EPOCH_DAY) * 24 * 3600, eval(unixTimeStampOf(DSL.literal(new ExprDateValue(date)))).longValue());
  }

  private static Stream<Arguments> getDateTimeSamples() {
    return Stream.of(
        Arguments.of(LocalDateTime.of(1984, 1, 1, 1, 1)),
        Arguments.of(LocalDateTime.of(2000, 2, 29, 22, 54)),
        Arguments.of(LocalDateTime.of(1999, 12, 31, 23, 59)),
        Arguments.of(LocalDateTime.of(2004, 2, 29, 7, 40)),
        Arguments.of(LocalDateTime.of(2100, 2, 28, 13, 14)),
        Arguments.of(LocalDateTime.of(2012, 2, 21, 0, 0))
    );
  }

  @ParameterizedTest
  @MethodSource("getDateTimeSamples")
  public void checkOfDateTime(LocalDateTime dateTime) {
    assertEquals(dateTime.toEpochSecond(ZoneOffset.UTC), unixTimeStampOf(dateTime));
    assertEquals(dateTime.toEpochSecond(ZoneOffset.UTC), eval(unixTimeStampOf(DSL.literal(new ExprDatetimeValue(dateTime)))).longValue());
  }

  private static Stream<Arguments> getInstantSamples() {
    return getDateTimeSamples().map(v -> Arguments.of(((LocalDateTime)v.get()[0]).toInstant(ZoneOffset.UTC)));
  }

  @ParameterizedTest
  @MethodSource("getInstantSamples")
  public void checkOfDateTime(Instant instant) {
    assertEquals(instant.getEpochSecond(), unixTimeStampOf(instant));
    assertEquals(instant.getEpochSecond(), eval(unixTimeStampOf(DSL.literal(new ExprTimestampValue(instant)))).longValue());
  }

  // formats: YYMMDD, YYMMDDhhmmss[.uuuuuu], YYYYMMDD, or YYYYMMDDhhmmss[.uuuuuu]
  // use BigDecimal, because double can't fit such big values
  private static Stream<Arguments> getDoubleSamples() {
    return Stream.of(
        Arguments.of(new BigDecimal("840101.")),
        Arguments.of(new BigDecimal("840101112233.")),
        Arguments.of(new BigDecimal("840101112233.123456")),
        Arguments.of(new BigDecimal("19840101.")),
        Arguments.of(new BigDecimal("19840101000000.")),
        Arguments.of(new BigDecimal("19840101112233.")),
        Arguments.of(new BigDecimal("19840101112233.123456"))
    );
  }

  @ParameterizedTest
  @MethodSource("getDoubleSamples")
  public void checkOfDoubleFormats(BigDecimal value) {
    LocalDateTime valueDt = LocalDateTime.MIN;
    var format = new DecimalFormat("0.#");
    format.setMinimumFractionDigits(0);
    format.setMaximumFractionDigits(6);
    var valueStr = format.format(value);
    switch (valueStr.length()) {
      case 6: valueStr = "19" + valueStr;
      case 8: valueStr += "000000";
      case 14: valueDt = LocalDateTime.parse(valueStr, DateTimeFormatter.ofPattern("yyyyMMddHHmmss")); break;
      case 12:
      case 19: valueStr = "19" + valueStr;
      case 21: valueDt = LocalDateTime.parse(valueStr, DateTimeFormatter.ofPattern("yyyyMMddHHmmss[.SSSSSS]")); break;
    }
    assertEquals(valueDt.toEpochSecond(ZoneOffset.UTC), unixTimeStampOf(value.doubleValue()), 1d, format.format(value));
    assertEquals(valueDt.toEpochSecond(ZoneOffset.UTC), eval(unixTimeStampOf(DSL.literal(new ExprDoubleValue(value.doubleValue())))).longValue(), 1d, format.format(value));
  }

  @Test
  public void checkOfDouble() {
    // 19991231235959.99 passed ok, but 19991231235959.999999 rounded to ...60.0 which is incorrect
    // It is a double type limitation
    var valueDt = LocalDateTime.of(1999, 12, 31, 23, 59, 59, 999_999_999);
    assertEquals(valueDt.toEpochSecond(ZoneOffset.UTC), unixTimeStampOf(19991231235959.99d), 1d);
  }

  @Test
  public void checkYearLessThan1970() {
    assertNotEquals(0, unixTimeStamp());
    assertEquals(0, unixTimeStampOf(LocalDate.of(1961, 4, 12)));
    assertEquals(0, unixTimeStampOf(LocalDateTime.of(1961, 4, 12, 9, 7, 0)));
    assertEquals(0, unixTimeStampOf(Instant.ofEpochMilli(-1)));
    assertEquals(0, unixTimeStampOf(LocalDateTime.of(1970, 1, 1, 0, 0, 0)));
    assertEquals(1, unixTimeStampOf(LocalDateTime.of(1970, 1, 1, 0, 0, 1)));
    assertEquals(0, unixTimeStampOf(19610412d));
    assertEquals(0, unixTimeStampOf(19610412090700d));
  }

  @Test
  public void checkMaxValue() {
    // MySQL returns 0 for values above
    //     '3001-01-19 03:14:07.999999' UTC (corresponding to 32536771199.999999 seconds).
    assertEquals(0, unixTimeStampOf(LocalDate.of(3001, 1, 20)));
    assertNotEquals(0d, unixTimeStampOf(LocalDate.of(3001, 1, 18)));
    assertEquals(0, unixTimeStampOf(LocalDateTime.of(3001, 1, 20, 3, 14, 8)));
    assertNotEquals(0d, unixTimeStampOf(LocalDateTime.of(3001, 1, 18, 3, 14, 7)));
    assertEquals(0, unixTimeStampOf(Instant.ofEpochSecond(32536771199L + 1)));
    assertNotEquals(0d, unixTimeStampOf(Instant.ofEpochSecond(32536771199L)));
    assertEquals(0, unixTimeStampOf(30010120d));
    assertNotEquals(0d, unixTimeStampOf(30010118d));
  }

  private static Stream<Arguments> getInvalidDoubleSamples() {
    return Stream.of(
        //invalid dates
        Arguments.of(19990231.d),
        Arguments.of(19991320.d),
        Arguments.of(19991232.d),
        Arguments.of(19990013.d),
        Arguments.of(19990931.d),
        Arguments.of(990231.d),
        Arguments.of(991320.d),
        Arguments.of(991232.d),
        Arguments.of(990013.d),
        Arguments.of(990931.d),
        Arguments.of(9990102.d),
        Arguments.of(99102.d),
        Arguments.of(9912.d),
        Arguments.of(199912.d),
        Arguments.of(1999102.d),
        //same as above, but with valid time
        Arguments.of(19990231112233.d),
        Arguments.of(19991320112233.d),
        Arguments.of(19991232112233.d),
        Arguments.of(19990013112233.d),
        Arguments.of(19990931112233.d),
        Arguments.of(990231112233.d),
        Arguments.of(991320112233.d),
        Arguments.of(991232112233.d),
        Arguments.of(990013112233.d),
        Arguments.of(990931112233.d),
        Arguments.of(9990102112233.d),
        Arguments.of(99102112233.d),
        Arguments.of(9912112233.d),
        Arguments.of(199912112233.d),
        Arguments.of(1999102112233.d),
        //invalid time
        Arguments.of(19840101242233.d),
        Arguments.of(19840101116033.d),
        Arguments.of(19840101112260.d),
        Arguments.of(1984010111223.d),
        Arguments.of(198401011123.d),
        Arguments.of(19840101123.d),
        Arguments.of(1984010113.d),
        Arguments.of(198401011.d),
        //same, but with short date
        Arguments.of(840101242233.d),
        Arguments.of(840101116033.d),
        Arguments.of(840101112260.d),
        Arguments.of(84010111223.d),
        Arguments.of(8401011123.d),
        Arguments.of(840101123.d),
        Arguments.of(8401011.d),
        //misc
        Arguments.of(0d),
        Arguments.of(-1d),
        Arguments.of(42d)
    );
  }

  @ParameterizedTest
  @MethodSource("getInvalidDoubleSamples")
  public void checkInvalidDoubleCausesNull(Double value) {
    assertEquals(ExprNullValue.of(), unixTimeStampOf(DSL.literal(new ExprDoubleValue(value))).valueOf(null), new DecimalFormat("0.#").format(value));
  }
}
