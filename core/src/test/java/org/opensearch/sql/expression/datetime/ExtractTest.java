/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import java.time.LocalDate;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

import static java.time.temporal.ChronoField.ALIGNED_WEEK_OF_YEAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;

@ExtendWith(MockitoExtension.class)
class ExtractTest extends ExpressionTestBase {
  private static Stream<Arguments> getDatetimeTestDataForExtractFunction() {
    return Stream.of(
        Arguments.of("DAY_MICROSECOND", 11101112123000L),
        Arguments.of("DAY_SECOND", 11101112),
        Arguments.of("DAY_MINUTE", 111011),
        Arguments.of("DAY_HOUR", 1110)
    );
  }

  private static Stream<Arguments> getTimeTestDataForExtractFunction() {
    return Stream.of(
        Arguments.of("MICROSECOND", 123000),
        Arguments.of("SECOND", 12),
        Arguments.of("MINUTE", 11),
        Arguments.of("HOUR", 10),
        Arguments.of("SECOND_MICROSECOND", 12123000),
        Arguments.of("MINUTE_MICROSECOND", 1112123000),
        Arguments.of("MINUTE_SECOND", 1112),
        Arguments.of("HOUR_MICROSECOND", 101112123000L),
        Arguments.of("HOUR_SECOND", 101112),
        Arguments.of("HOUR_MINUTE", 1011)
    );
  }

  private static Stream<Arguments> getDateTestDataForExtractFunction() {
    return Stream.of(
        Arguments.of("DAY", 11),
        Arguments.of("WEEK", 6),
        Arguments.of("MONTH", 2),
        Arguments.of("QUARTER", 1),
        Arguments.of("YEAR", 2023),
        Arguments.of("YEAR_MONTH", 202302)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource({
      "getDatetimeTestDataForExtractFunction",
      "getTimeTestDataForExtractFunction",
      "getDateTestDataForExtractFunction"})
  public void testExtractWithDatetime(String part, long expected) {
    

    FunctionExpression datetimeExpression = DSL.extract(
        DSL.literal(part),
        DSL.literal(new ExprDatetimeValue("2023-02-11 10:11:12.123")));

    assertEquals(LONG, datetimeExpression.type());
    assertEquals(expected, eval(datetimeExpression).longValue());
    assertEquals(
        String.format("extract(\"%s\", DATETIME '2023-02-11 10:11:12.123')", part),
        datetimeExpression.toString());
  }

  private void datePartWithTimeArgQuery(String part, long expected) {
    ExprTimeValue timeValue = new ExprTimeValue("10:11:12.123");

    FunctionExpression datetimeExpression = DSL.extract(
        functionProperties,
        DSL.literal(part),
        DSL.literal(timeValue));

    assertEquals(LONG, datetimeExpression.type());
    assertEquals(expected,
        eval(datetimeExpression).longValue());
  }


  @Test
  public void testExtractDatePartWithTimeType() {
    


    datePartWithTimeArgQuery(
        "DAY",
        LocalDate.now(functionProperties.getQueryStartClock()).getDayOfMonth());

    datePartWithTimeArgQuery(
        "WEEK",
        LocalDate.now(functionProperties.getQueryStartClock()).get(ALIGNED_WEEK_OF_YEAR));

    datePartWithTimeArgQuery(
        "MONTH",
        LocalDate.now(functionProperties.getQueryStartClock()).getMonthValue());

    datePartWithTimeArgQuery(
        "YEAR",
        LocalDate.now(functionProperties.getQueryStartClock()).getYear());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getDateTestDataForExtractFunction")
  public void testExtractWithDate(String part, long expected) {
    

    FunctionExpression datetimeExpression = DSL.extract(
        DSL.literal(part),
        DSL.literal(new ExprDateValue("2023-02-11")));

    assertEquals(LONG, datetimeExpression.type());
    assertEquals(expected, eval(datetimeExpression).longValue());
    assertEquals(
        String.format("extract(\"%s\", DATE '2023-02-11')", part),
        datetimeExpression.toString());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getTimeTestDataForExtractFunction")
  public void testExtractWithTime(String part, long expected) {
    

    FunctionExpression datetimeExpression = DSL.extract(
        functionProperties,
        DSL.literal(part),
        DSL.literal(new ExprTimeValue("10:11:12.123")));

    assertEquals(LONG, datetimeExpression.type());
    assertEquals(expected, eval(datetimeExpression).longValue());
    assertEquals(
        String.format("extract(\"%s\", TIME '10:11:12.123')", part),
        datetimeExpression.toString());
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}
