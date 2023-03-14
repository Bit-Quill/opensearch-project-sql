/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

class TimeStampAddTest extends ExpressionTestBase {

  private static Stream<Arguments> getTestDataForTimestampAdd() {
    return Stream.of(
        Arguments.of("MINUTE", 1, new ExprStringValue("2003-01-02"),
            "2003-01-02 00:01:00"),
        Arguments.of("WEEK", 1, new ExprStringValue("2003-01-02"),
            "2003-01-09"),
        //Date
        Arguments.of("MINUTE", 1, new ExprDateValue("2003-01-02"),
            "2003-01-02 00:01:00"),
        Arguments.of("WEEK", 1, new ExprDateValue("2003-01-02"),
            "2003-01-09"),
        //Datetime
        Arguments.of("MINUTE", 1, new ExprDatetimeValue("2003-01-02 00:00:00"),
            "2003-01-02 00:01:00"),
        Arguments.of("WEEK", 1, new ExprDatetimeValue("2003-01-02 00:00:00"),
            "2003-01-09"),
        //timstamp
        Arguments.of("MINUTE", 1, new ExprTimestampValue("2003-01-02 00:00:00"),
            "2003-01-02 00:01:00"),
        Arguments.of("WEEK", 1, new ExprTimestampValue("2003-01-02 00:00:00"),
            "2003-01-09"),
        //time
        //Note: Adding date part to time argument requires function properties and is handled in another test.
        Arguments.of("MINUTE", 1, new ExprTimestampValue("10:11:12"),
            "10:12:12"),
        //Cases surrounding leap year
        Arguments.of("SECOND", 1, new ExprTimestampValue("2020-02-28 23:59:59"),
            "2020-02-29 00:00:00"),
        Arguments.of("MINUTE", 1, new ExprTimestampValue("2020-02-28 23:59:59"),
            "2020-02-29 00:00:59"),
        Arguments.of("HOUR", 1, new ExprTimestampValue("2020-02-28 23:59:59"),
            "2020-02-29 00:59:59"),
        Arguments.of("DAY", 1, new ExprTimestampValue("2020-02-28 23:59:59"),
            "2020-02-29 23:59:59"),
        Arguments.of("WEEK", 1, new ExprTimestampValue("2020-02-28 23:59:59"),
            "2020-03-06 23:59:59"),
        //Cases surrounding end-of-year
        Arguments.of("SECOND", 1, new ExprTimestampValue("2020-12-31 23:59:59"),
            "2021-01-01 00:00:00"),
        Arguments.of("MINUTE", 1, new ExprTimestampValue("2020-12-31 23:59:59"),
            "2021-01-01 00:00:59"),
        Arguments.of("HOUR", 1, new ExprTimestampValue("2020-12-31 23:59:59"),
            "2021-01-01 00:59:59"),
        Arguments.of("DAY", 1, new ExprTimestampValue("2020-12-31 23:59:59"),
            "2021-01-01 23:59:59"),
        Arguments.of("WEEK", 1, new ExprTimestampValue("2020-12-31 23:59:59"),
            "2021-01-07 23:59:59")

    );
  }

  private static FunctionExpression timestampaddQuery(String unit, int amount, ExprValue datetimeExpr) {
    return DSL.timestampadd(
        DSL.literal(unit),
        DSL.literal(new ExprIntegerValue(amount)),
        DSL.literal(datetimeExpr)
    );
  }

  @ParameterizedTest
  public void testTimestampadd(String unit, int amount, ExprValue datetimeExpr, String expected) {
    FunctionExpression expr = timestampaddQuery(unit, amount, datetimeExpr);
    assertEquals(new ExprDatetimeValue(expected), eval(expr));
  }

  @Test
  public void testAddingDatePartToTime() {
    String interval = "WEEK";
    int addedInterval = 1;
    String timeArg = "10:11:12";
    FunctionExpression expr = DSL.timestampadd(
        functionProperties,
        DSL.literal(interval),
        DSL.literal(new ExprIntegerValue(addedInterval)),
        DSL.literal(new ExprTimeValue(timeArg))
    );

    LocalDate today = LocalDate.now().plusWeeks(addedInterval);
    LocalDateTime expected = LocalDateTime.of(today, LocalTime.parse(timeArg));

    assertEquals(new ExprDatetimeValue(expected), eval(expr));
  }

  //TODO: Add test to compare outputs when using a string/timestamp/datetime for 3rd argument.
  @Test
  public void testDifferentInputTypesHaveSameResult() {
    String part = "SECOND";
    int amount = 1;
    FunctionExpression dateExpr = timestampaddQuery(part, amount, new ExprDateValue("2000-01-01"));
    FunctionExpression stringExpr = timestampaddQuery(part, amount, new ExprStringValue("2000-01-01"));
    FunctionExpression datetimeExpr = timestampaddQuery(
        part,
        amount,
        new ExprDatetimeValue("2000-01-01 00:00:00"));
    FunctionExpression timestampExpr = timestampaddQuery(
        part,
        amount,
        new ExprTimestampValue("2000-01-01 00:00:00"));

    assertAll(
        () -> assertEquals(eval(dateExpr), eval(stringExpr)),
        () -> assertEquals(eval(dateExpr), eval(datetimeExpr)),
        () -> assertEquals(eval(dateExpr), eval(timestampExpr))
    );
  }

  @Test
  public void testInvalidArguments() {
    FunctionExpression expr1 = timestampaddQuery("INVALID", 1, new ExprDateValue("2000-01-01"));
    FunctionExpression expr2 = timestampaddQuery("WEEK", 1, new ExprStringValue("2000-13-01"));
    FunctionExpression expr3 = timestampaddQuery("WEEK", 1, new ExprStringValue("2000-01-40"));

    assertAll(
        () -> assertThrows(SemanticCheckException.class, () -> eval(expr1)),
        () -> assertThrows(SemanticCheckException.class, () -> eval(expr2)),
        () -> assertThrows(SemanticCheckException.class, () -> eval(expr3))
    );
  }

  //TODO: Add test to check for null value return???
  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}
