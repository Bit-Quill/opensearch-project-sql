/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnixTwoWayConversionTest {

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

  private FunctionExpression fromUnixTime(Expression value) {
    var repo = new ExpressionConfig().functionRepository();
    var func = repo.resolve(new FunctionSignature(new FunctionName("from_unixtime"),
        List.of(value.type())));
    return (FunctionExpression)func.apply(List.of(value));
  }

  private LocalDateTime fromUnixTime(Long value) {
    return fromUnixTime(DSL.literal(value)).valueOf(null).datetimeValue();
  }

  private LocalDateTime fromUnixTime(Double value) {
    return fromUnixTime(DSL.literal(value)).valueOf(null).datetimeValue();
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }

  @Test
  public void checkConvertNow() {
    assertEquals(LocalDateTime.now(ZoneId.of("UTC")).withNano(0), fromUnixTime(unixTimeStamp()));
    assertEquals(LocalDateTime.now(ZoneId.of("UTC")).withNano(0), eval(fromUnixTime(unixTimeStampExpr())).datetimeValue());
  }

  private static Stream<Arguments> getDoubleSamples() {
    return Stream.of(
        Arguments.of(0.123d),
        Arguments.of(100500.100500d),
        Arguments.of(1447430881.564d),
        Arguments.of(2147483647.451232d),
        Arguments.of(1662577241.d)
    );
  }

  @ParameterizedTest
  @MethodSource("getDoubleSamples")
  public void convertEpoch2DateTime2Epoch(Double value) {
    assertEquals(value, unixTimeStampOf(fromUnixTime(value)));
    assertEquals(value, eval(unixTimeStampOf(fromUnixTime(DSL.literal(new ExprDoubleValue(value))))).doubleValue());

    assertEquals(Math.round(value) + 0d, unixTimeStampOf(fromUnixTime(Math.round(value))));
    assertEquals(Math.round(value) + 0d, eval(unixTimeStampOf(fromUnixTime(DSL.literal(new ExprLongValue(Math.round(value)))))).doubleValue());
  }

  private static Stream<Arguments> getDateTimeSamples() {
    return Stream.of(
        Arguments.of(LocalDateTime.of(1984, 1, 1, 1, 1)),
        Arguments.of(LocalDateTime.of(2000, 2, 29, 22, 54)),
        Arguments.of(LocalDateTime.of(1999, 12, 31, 23, 59, 59)),
        Arguments.of(LocalDateTime.of(2004, 2, 29, 7, 40)),
        Arguments.of(LocalDateTime.of(2100, 2, 28, 13, 14, 15)),
        Arguments.of(LocalDateTime.of(2012, 2, 21, 0, 0, 17))
    );
  }

  @ParameterizedTest
  @MethodSource("getDateTimeSamples")
  public void convertDateTime2Epoch2DateTime(LocalDateTime value) {
    assertEquals(value, fromUnixTime(unixTimeStampOf(value)));
    assertEquals(value, eval(fromUnixTime(unixTimeStampOf(DSL.literal(new ExprDatetimeValue(value))))).datetimeValue());
  }
}
