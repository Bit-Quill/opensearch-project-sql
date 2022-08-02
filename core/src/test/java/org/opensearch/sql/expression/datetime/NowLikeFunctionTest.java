/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class NowLikeFunctionTest extends ExpressionTestBase {
  private static Stream<Arguments> functionNames() {
    var dsl = new DSL(new ExpressionConfig().functionRepository());
    return Stream.of(
        Arguments.of((Function<Expression[], FunctionExpression>)dsl::now,
            "now", DATETIME, true, (Supplier<Temporal>)LocalDateTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>)dsl::current_timestamp,
            "current_timestamp", DATETIME, true, (Supplier<Temporal>)LocalDateTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>)dsl::localtimestamp,
            "localtimestamp", DATETIME, true, (Supplier<Temporal>)LocalDateTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>)dsl::localtime,
            "localtime", DATETIME, true, (Supplier<Temporal>)LocalDateTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>)dsl::sysdate,
            "sysdate", DATETIME, true, (Supplier<Temporal>)LocalDateTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>)dsl::curtime,
            "curtime", TIME, true, (Supplier<Temporal>)LocalTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>)dsl::current_time,
            "current_time", TIME, true, (Supplier<Temporal>)LocalTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>)dsl::curdate,
            "curdate", DATE, false, (Supplier<Temporal>)LocalDate::now),
        Arguments.of((Function<Expression[], FunctionExpression>)dsl::current_date,
            "current_date", DATE, false, (Supplier<Temporal>)LocalDate::now));
  }

  private ExprCoreType getCastRule(ExprCoreType from) {
    switch (from) {
      case DATETIME:
      case TIME: return DOUBLE;
      case DATE: return INTEGER;
    }
    // unreachable code
    throw new IllegalArgumentException(String.format("%s", from));
  }

  private Temporal extractValue(FunctionExpression func) {
    switch ((ExprCoreType)func.type()) {
      case DATE: return func.valueOf(null).dateValue();
      case DATETIME: return func.valueOf(null).datetimeValue();
      case TIME: return func.valueOf(null).timeValue();
    }
    // unreachable code
    throw new IllegalArgumentException(String.format("%s", func.type()));
  }

  private long getDiff(Temporal sample, Temporal reference) {
    if (sample instanceof LocalDate) {
      return Period.between((LocalDate) sample, (LocalDate) reference).getDays();
    }
    return Duration.between(sample, reference).toSeconds();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("functionNames")
  public void the_test(Function<Expression[], FunctionExpression> function,
                       @SuppressWarnings("unused")  // Used in the test name above
                       String name,
                       ExprCoreType resType,
                       Boolean hasFsp,
                       Supplier<Temporal> referenceGetter) {
    // Check return types:
    // `func()`
    FunctionExpression expr = function.apply(new Expression[]{});
    assertEquals(resType, expr.type());
    if (hasFsp) {
      // `func(fsp = 0)`
      expr = function.apply(new Expression[]{DSL.literal(0)});
      assertEquals(resType, expr.type());
      // `func(fsp = 6)`
      expr = function.apply(new Expression[]{DSL.literal(6)});
      assertEquals(resType, expr.type());
    }

    // Check how calculations are precise:
    // `func()`
    assertTrue(Math.abs(getDiff(
            extractValue(function.apply(new Expression[]{})),
            referenceGetter.get()
        )) <= 1);
    if (hasFsp) {
      // `func(fsp)`
      assertTrue(Math.abs(getDiff(
              extractValue(function.apply(new Expression[]{DSL.literal(0)})),
              referenceGetter.get()
      )) <= 1);
    }
  }
}
