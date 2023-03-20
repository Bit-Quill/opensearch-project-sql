/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.function.FunctionProperties;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TimeStampDiffTest extends ExpressionTestBase {

  private static Stream<Arguments> getTestDataForTimestampDiff() {
    return Stream.of(
        //Test Date
        Arguments.of(
            "DAY",
            new ExprDateValue("2000-01-01 00:00:00"),
            new ExprDateValue("2000-01-01"),
            0),
        Arguments.of(
            "DAY",
            new ExprDateValue("2000-01-01"),
            new ExprDateValue("2000-01-06"),
            5),
        Arguments.of(
            "DAY",
            new ExprDateValue("2000-01-06"),
            new ExprDateValue("2000-01-01"),
            -5),

        //Test Datetime
        Arguments.of(
            "DAY",
            new ExprDatetimeValue("2000-01-01 00:00:00"),
            new ExprDatetimeValue("2000-01-01 00:00:00"),
            0),
        Arguments.of(
            "DAY",
            new ExprDatetimeValue("2000-01-01 00:00:00"),
            new ExprDatetimeValue("2000-01-06 00:00:00"),
            5),
        Arguments.of(
            "DAY",
            new ExprDatetimeValue("2000-01-06 00:00:00"),
            new ExprDatetimeValue("2000-01-01 00:00:00"),
            -5),

        //Test Timestamp
        Arguments.of(
            "DAY",
            new ExprTimestampValue("2000-01-01 00:00:00"),
            new ExprTimestampValue("2000-01-01 00:00:00"),
            0),
        Arguments.of(
            "DAY",
            new ExprTimestampValue("2000-01-01 00:00:00"),
            new ExprTimestampValue("2000-01-06 00:00:00"),
            5),
        Arguments.of(
            "DAY",
            new ExprTimestampValue("2000-01-06 00:00:00"),
            new ExprTimestampValue("2000-01-01 00:00:00"),
            -5),

        //Test String
        Arguments.of(
            "DAY",
            new ExprStringValue("2000-01-01 00:00:00"),
            new ExprStringValue("2000-01-01 00:00:00"),
            0),
        Arguments.of(
            "DAY",
            new ExprStringValue("2000-01-01 00:00:00"),
            new ExprStringValue("2000-01-06 00:00:00"),
            5),
        Arguments.of(
            "DAY",
            new ExprStringValue("2000-01-06 00:00:00"),
            new ExprStringValue("2000-01-01 00:00:00"),
            -5),

        //Test with mismatched argument types
        Arguments.of(
            "DAY",
            new ExprTimestampValue("2000-01-01 00:00:00"),
            new ExprStringValue("2000-01-06 00:00:00"),
            5),
        Arguments.of(
            "DAY",
            new ExprDatetimeValue("2000-01-01 00:00:00"),
            new ExprTimestampValue("2000-01-06 00:00:00"),
            5),
        Arguments.of(
            "DAY",
            new ExprDateValue("2000-01-01 00:00:00"),
            new ExprDatetimeValue("2000-01-06 00:00:00"),
            5),
        Arguments.of(
            "DAY",
            new ExprStringValue("2000-01-01 00:00:00"),
            new ExprDateValue("2000-01-06 00:00:00"),
            5),

        //Test for all interval options
        Arguments.of(
            "MICROSECOND",
            new ExprDatetimeValue("2000-01-06 00:00:00"),
            new ExprDatetimeValue("2000-01-06 00:00:00.000123"),
            123),
        Arguments.of(
            "SECOND",
            new ExprDatetimeValue("2000-01-06 00:00:00"),
            new ExprDatetimeValue("2000-01-06 00:00:12"),
            12),
        Arguments.of(
            "MINUTE",
            new ExprDatetimeValue("2000-01-06 00:00:00"),
            new ExprDatetimeValue("2000-01-06 00:11:12"),
            11),
        Arguments.of(
            "HOUR",
            new ExprDatetimeValue("2000-01-06 00:00:00"),
            new ExprDatetimeValue("2000-01-06 10:11:12"),
            10),
        Arguments.of(
            "DAY",
            new ExprDatetimeValue("2000-01-01 00:00:00"),
            new ExprDatetimeValue("2000-01-06 10:11:12"),
            5),
        Arguments.of(
            "WEEK",
            new ExprDatetimeValue("2000-01-01 00:00:00"),
            new ExprDatetimeValue("2000-01-31 10:11:12"),
            4),
        Arguments.of(
            "MONTH",
            new ExprDatetimeValue("2000-01-01 00:00:00"),
            new ExprDatetimeValue("2000-12-31 10:11:12"),
            11),
        Arguments.of(
            "QUARTER",
            new ExprDatetimeValue("2000-01-01 00:00:00"),
            new ExprDatetimeValue("2000-12-31 10:11:12"),
            3),
        Arguments.of(
            "YEAR",
            new ExprDatetimeValue("1999-01-01 00:00:00"),
            new ExprDatetimeValue("2000-12-31 10:11:12"),
            1),

        //Test around Leap Year
        Arguments.of(
            "DAY",
            new ExprDatetimeValue("2019-02-28 00:00:00"),
            new ExprDatetimeValue("2019-03-01 00:00:00"),
            1),
        Arguments.of(
            "DAY",
            new ExprDatetimeValue("2020-02-28 00:00:00"),
            new ExprDatetimeValue("2020-03-01 00:00:00"),
            2),

        //Test around year change
        Arguments.of(
            "SECOND",
            new ExprDatetimeValue("2019-12-31 23:59:59"),
            new ExprDatetimeValue("2020-01-01 00:00:00"),
            1),
        Arguments.of(
            "DAY",
            new ExprDatetimeValue("2019-12-31 23:59:59"),
            new ExprDatetimeValue("2020-01-01 00:00:00"),
            0),
        Arguments.of(
            "DAY",
            new ExprDatetimeValue("2019-12-31 00:00:00"),
            new ExprDatetimeValue("2020-01-01 00:00:00"),
            1)
    );
  }

  private static FunctionExpression timestampdiffQuery(FunctionProperties functionProperties,
                                                       String unit,
                                                       ExprValue datetimeExpr1,
                                                      ExprValue datetimeExpr2) {
    return DSL.timestampdiff(
        functionProperties,
        DSL.literal(unit),
        DSL.literal(datetimeExpr1),
        DSL.literal(datetimeExpr2)
    );
  }

  @ParameterizedTest
  @MethodSource("getTestDataForTimestampDiff")
  public void testTimestampdiff(String unit,
                               ExprValue datetimeExpr1,
                               ExprValue datetimeExpr2,
                               long expected) {
    FunctionExpression expr = timestampdiffQuery(
        functionProperties,
        unit,
        datetimeExpr1,
        datetimeExpr2);
    assertEquals(expected, eval(expr).longValue());
  }

  private static Stream<Arguments> getUnits() {
    return Stream.of(
        Arguments.of("MICROSECOND"),
        Arguments.of("SECOND"),
        Arguments.of("MINUTE"),
        Arguments.of("HOUR"),
        Arguments.of("DAY"),
        Arguments.of("WEEK"),
        Arguments.of("MONTH"),
        Arguments.of("QUARTER"),
        Arguments.of("YEAR")
    );
  }

  //Test that Time arg uses today's date with all interval/part arguments
  @ParameterizedTest
  @MethodSource("getUnits")
  public void testTimestampDiffWithTimeType(String unit) {
    LocalDate dateToday = LocalDate.now();
    LocalTime timeArg = LocalTime.of(10, 11, 12);
    FunctionExpression expr = timestampdiffQuery(
        functionProperties,
        unit,
        new ExprDatetimeValue(LocalDateTime.of(dateToday, timeArg)),
        new ExprTimeValue(timeArg)
    );
    assertEquals(0L, eval(expr).longValue());
  }

  private static Stream<Arguments> getTimestampDiffInvalidArgs() {
    return Stream.of(
        Arguments.of("SECOND", "2023-13-01 10:11:12", "2000-01-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-40 10:11:12", "2000-01-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 25:11:12", "2000-01-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:70:12", "2000-01-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:70", "2000-01-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:12", "2000-13-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:12", "2000-01-40 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:12", "2000-01-01 25:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:12", "2000-01-01 10:70:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:12", "2000-01-01 10:11:70")
    );
  }

  @ParameterizedTest
  @MethodSource("getTimestampDiffInvalidArgs")
  public void testTimestampDiffWithInvalidTimeArgs(String unit, String arg1, String arg2) {
    FunctionExpression expr = timestampdiffQuery(
        functionProperties,
        unit,
        new ExprStringValue(arg1),
        new ExprStringValue(arg2)
    );
    assertThrows(SemanticCheckException.class, () -> eval(expr));
  }

  @Test
  public void testTimestampDiffWithInvalidPartReturnsNull() {
    FunctionExpression expr = timestampdiffQuery(
        functionProperties,
            "INVALID",
        new ExprStringValue("2023-01-01 10:11:12"),
        new ExprStringValue("2000-01-01 10:11:12")
    );
    assertEquals(ExprNullValue.of(), eval(expr));
  }

  //Test that different input types have the same result
  @Test
  public void testDifferentInputTypesHaveSameResult() {
    String part = "SECOND";
    FunctionExpression dateExpr = timestampdiffQuery(
        functionProperties,
        part,
        new ExprDateValue("2000-01-01"),
        new ExprDateValue("2000-01-02"));

    FunctionExpression stringExpr = timestampdiffQuery(
        functionProperties,
        part,
        new ExprStringValue("2000-01-01 00:00:00"),
        new ExprStringValue("2000-01-02 00:00:00"));

    FunctionExpression datetimeExpr = timestampdiffQuery(
        functionProperties,
        part,
        new ExprDatetimeValue("2000-01-01 00:00:00"),
        new ExprDatetimeValue("2000-01-02 00:00:00"));

    FunctionExpression timestampExpr = timestampdiffQuery(
        functionProperties,
        part,
        new ExprTimestampValue("2000-01-01 00:00:00"),
        new ExprTimestampValue("2000-01-02 00:00:00"));

    assertAll(
        () -> assertEquals(eval(dateExpr), eval(stringExpr)),
        () -> assertEquals(eval(dateExpr), eval(datetimeExpr)),
        () -> assertEquals(eval(dateExpr), eval(timestampExpr))
    );
  }
  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}