/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.UNDEFINED;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;

@ExtendWith(MockitoExtension.class)
class StrToDateTest extends ExpressionTestBase {

  @Mock
  Environment<Expression, ExprValue> env;

  private static Stream<Arguments> getTestDataForStrToDate() {
    return Stream.of(
        //Date arguments
        Arguments.of(
            "01,5,2013",
            "%d,%m,%Y",
            new ExprDatetimeValue("2013-05-01 00:00:00"),
            DATETIME),
        Arguments.of(
            "May 1, 2013",
            "%M %d, %Y",
            new ExprDatetimeValue("2013-05-01 00:00:00"),
            DATETIME),
        Arguments.of(
            "May 1, 2013 - 9,23,11",
            "%M %d, %Y - %h,%i,%s",
            new ExprDatetimeValue("2013-05-01 09:23:11"),
            DATETIME),

        //Invalid Arguments (should return null)
        Arguments.of(
            "a09:30:17",
            "a%h:%i:%s",
            ExprNullValue.of(),
            UNDEFINED),
        Arguments.of(
            "abc",
            "abc",
            ExprNullValue.of(),
            UNDEFINED),
        Arguments.of(
            "9",
            "%m",
            ExprNullValue.of(),
            UNDEFINED),
        Arguments.of(
            "9",
            "%s",
            ExprNullValue.of(),
            UNDEFINED)
    );
  }

  @ParameterizedTest(name = "{0} | {1}")
  @MethodSource("getTestDataForStrToDate")
  public void test_str_to_date(
      String datetime,
      String format,
      ExprValue expectedResult,
      ExprCoreType expectedType) {

    FunctionExpression expression = DSL.str_to_date(
        functionProperties,
        DSL.literal(new ExprStringValue(datetime)),
        DSL.literal(new ExprStringValue(format)));

    ExprValue result = eval(expression);

    assertEquals(expectedType, result.type());
    assertEquals(expectedResult, result);
  }

  private static LocalDateTime getExpectedTimeResult(int hour, int minute, int seconds) {
    return LocalDateTime.of(
        LocalDate.now().getYear(),
        LocalDate.now().getMonthValue(),
        LocalDate.now().getDayOfMonth(),
        hour,
        minute,
        seconds
    );
  }

  @Test
  public void test_str_to_date_with_time_type() {
    final int HOURS = 9;
    final int MINUTES = 23;
    final int SECONDS = 11;

    FunctionExpression expression = DSL.str_to_date(
        functionProperties,
        DSL.literal(new ExprStringValue(String.format("%d,%d,%d", HOURS, MINUTES, SECONDS))),
        DSL.literal(new ExprStringValue("%h,%i,%s")));

    ExprValue result = eval(expression);

    assertEquals(DATETIME, result.type());
    assertEquals(getExpectedTimeResult(HOURS, MINUTES, SECONDS), result.datetimeValue());
  }

  @Test
  public void test_str_to_date_with_date_format() {

    LocalDateTime arg = LocalDateTime.of(2023, 2, 27, 10, 11,12);
    String format = "%Y,%m,%d %h,%i,%s";

    FunctionExpression dateFormatExpr = DSL.date_format(
        functionProperties,
        DSL.literal(new ExprDatetimeValue(arg)),
        DSL.literal(new ExprStringValue(format)));
    String dateFormatResult = eval(dateFormatExpr).stringValue();

    FunctionExpression strToDateExpr = DSL.str_to_date(
        functionProperties,
        DSL.literal(new ExprStringValue(dateFormatResult)),
        DSL.literal(new ExprStringValue(format)));
    LocalDateTime strToDateResult = eval(strToDateExpr).datetimeValue();

    assertEquals(arg, strToDateResult);
  }

  @Test
  public void test_str_to_date_with_time_format() {
    final int HOURS = 10;
    final int MINUTES = 11;
    final int SECONDS = 12;

    LocalTime arg = LocalTime.of(HOURS, MINUTES,SECONDS);
    String format = "%h,%i,%s";

    FunctionExpression dateFormatExpr = DSL.time_format(
        functionProperties,
        DSL.literal(new ExprTimeValue(arg)),
        DSL.literal(new ExprStringValue(format)));
    String timeFormatResult = eval(dateFormatExpr).stringValue();

    FunctionExpression strToDateExpr = DSL.str_to_date(
        functionProperties,
        DSL.literal(new ExprStringValue(timeFormatResult)),
        DSL.literal(new ExprStringValue(format)));
    LocalDateTime strToDateResult = eval(strToDateExpr).datetimeValue();

    assertEquals(
        getExpectedTimeResult(HOURS, MINUTES, SECONDS),
        strToDateResult);
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }
}
