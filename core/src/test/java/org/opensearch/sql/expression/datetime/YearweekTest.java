/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;

import java.time.LocalDate;
import java.util.stream.Stream;

import static java.time.temporal.ChronoField.ALIGNED_WEEK_OF_YEAR;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

@ExtendWith(MockitoExtension.class)
class YearweekTest extends ExpressionTestBase {

  @Mock
  Environment<Expression, ExprValue> env;

  private void yearweekQuery(String date, int mode, int expectedResult) {
    FunctionExpression expression = DSL
        .yearweek(
            functionProperties,
            DSL.literal(new ExprDateValue(date)), DSL.literal(mode));
    assertEquals(INTEGER, expression.type());
    assertEquals(String.format("yearweek(DATE '%s', %d)", date, mode), expression.toString());
    assertEquals(integerValue(expectedResult), eval(expression));
  }

  private static Stream<Arguments> getTestDataForYearweek() {
    //Test the behavior of different modes passed into the 'yearweek' function
    return Stream.of(
        Arguments.of("2019-01-05", 0, 201852),
        Arguments.of("2019-01-05", 1, 201901),
        Arguments.of("2019-01-05", 2, 201852),
        Arguments.of("2019-01-05", 3, 201901),
        Arguments.of("2019-01-05", 4, 201901),
        Arguments.of("2019-01-05", 5, 201853),
        Arguments.of("2019-01-05", 6, 201901),
        Arguments.of("2019-01-05", 7, 201853),
        Arguments.of("2019-01-06", 0, 201901),
        Arguments.of("2019-01-06", 1, 201901),
        Arguments.of("2019-01-06", 2, 201901),
        Arguments.of("2019-01-06", 3, 201901),
        Arguments.of("2019-01-06", 4, 201902),
        Arguments.of("2019-01-06", 5, 201853),
        Arguments.of("2019-01-06", 6, 201902),
        Arguments.of("2019-01-06", 7, 201853),
        Arguments.of("2019-01-07", 0, 201901),
        Arguments.of("2019-01-07", 1, 201902),
        Arguments.of("2019-01-07", 2, 201901),
        Arguments.of("2019-01-07", 3, 201902),
        Arguments.of("2019-01-07", 4, 201902),
        Arguments.of("2019-01-07", 5, 201901),
        Arguments.of("2019-01-07", 6, 201902),
        Arguments.of("2019-01-07", 7, 201901),
        Arguments.of("2000-01-01", 0, 199952),
        Arguments.of("2000-01-01", 2, 199952),
        Arguments.of("1999-12-31", 0, 199952)
    );
  }

  @ParameterizedTest(name = "{0} | {1}")
  @MethodSource("getTestDataForYearweek")
  public void testWeekyear(String date, int mode, int expected) {
    yearweekQuery(date, mode, expected);
  }

  @Test
  public void testYearWeekWithTimeType() {
    int week = LocalDate.now(functionProperties.getQueryStartClock()).get(ALIGNED_WEEK_OF_YEAR);
    int year = LocalDate.now(functionProperties.getQueryStartClock()).getYear();
    int expected = Integer.parseInt(String.format("%d%02d", year, week));

    FunctionExpression expression = DSL
        .yearweek(
            functionProperties,
            DSL.literal(new ExprTimeValue("10:11:12")), DSL.literal(0));

    assertEquals(expected, eval(expression).integerValue());
  }

  @Test
  public void testInvalidYearWeek() {
    assertAll(
        //test invalid month
        () -> assertThrows(
            SemanticCheckException.class,
            () -> yearweekQuery("2019-13-05 01:02:03", 0, 0)),
        //test invalid day
        () -> assertThrows(
            SemanticCheckException.class,
            () -> yearweekQuery("2019-01-50 01:02:03", 0, 0)),
        //test invalid leap year
        () -> assertThrows(
            SemanticCheckException.class,
            () -> yearweekQuery("2019-02-29 01:02:03", 0, 0))
    );
  }
  private ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }
}
