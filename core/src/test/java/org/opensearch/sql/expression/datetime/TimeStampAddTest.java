/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

class TimeStampAddTest extends ExpressionTestBase {

  private static Stream<Arguments> getTestDataForTimestampAdd() {
    return Stream.of(
        //TODO: Add more test cases
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
        Arguments.of("MINUTE", 1, new ExprTimestampValue("10:11:12"),
            "10:12:12"),
        //Note: Adding date part to time argument requires function properties and is handled in another test.
        //TODO: Add cases surrounding leap year
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
        //TODO: Add cases surround end-of-year
        Arguments.of("SECOND", 1, new ExprTimestampValue("2020-12-31 23:59:59"),
            "10:12:12"),
        Arguments.of("MINUTE", 1, new ExprTimestampValue("2020-12-31 23:59:59"),
            "10:12:12"),
        Arguments.of("HOUR", 1, new ExprTimestampValue("2020-12-31 23:59:59"),
            "10:12:12"),
        Arguments.of("DAY", 1, new ExprTimestampValue("2020-12-31 23:59:59"),
            "10:12:12"),
        Arguments.of("WEEK", 1, new ExprTimestampValue("2020-12-31 23:59:59"),
            "10:12:12")

    );
  }

  @ParameterizedTest
  public void testTimestampadd(String unit, int interval, ExprValue datetimeExpr, String expected) {
    FunctionExpression expr = DSL.timestampadd(
        DSL.literal(unit),
        DSL.literal(new ExprIntegerValue(interval)),
        DSL.literal(datetimeExpr)
    );

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

  //TODO: Add test for invalid arguments

  //TODO: Add test to check for null value return???
  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}
