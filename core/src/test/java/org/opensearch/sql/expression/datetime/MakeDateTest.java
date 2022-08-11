/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.missingValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;

import java.time.LocalDate;
import java.time.Year;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;

@ExtendWith(MockitoExtension.class)
public class MakeDateTest extends ExpressionTestBase {

  @Mock
  Environment<Expression, ExprValue> env;

  @Mock
  Expression nullRef;

  @Mock
  Expression missingRef;

  private FunctionExpression makedate(Expression year, Expression dayOfYear) {
    var repo = new ExpressionConfig().functionRepository();
    var func = repo.resolve(new FunctionSignature(new FunctionName("makedate"),
        List.of(DOUBLE, DOUBLE)));
    return (FunctionExpression)func.apply(List.of(year, dayOfYear));
  }

  private LocalDate makedate(Double year, Double dayOfYear) {
    return makedate(DSL.literal(year), DSL.literal(dayOfYear)).valueOf(null).dateValue();
  }

  @Test
  public void checkEdgeCases() {
    assertEquals(LocalDate.ofYearDay(2002, 1), makedate(2001., 366.),
        "No switch to the next year on getting 366th day of a non-leap year");
    assertEquals(LocalDate.ofYearDay(2005, 1), makedate(2004., 367.),
        "No switch to the next year on getting 367th day of a leap year");
    assertEquals(LocalDate.ofYearDay(2000, 42), makedate(0., 42.),
        "0 year is not interpreted as 2000 as in MySQL");
    assertEquals(nullValue(), eval(makedate(DSL.literal(-1.), DSL.literal(42.))),
        "Negative year doesn't produce NULL");
    assertEquals(nullValue(), eval(makedate(DSL.literal(42.), DSL.literal(-1.))),
        "Negative dayOfYear doesn't produce NULL");
    assertEquals(nullValue(), eval(makedate(DSL.literal(42.), DSL.literal(0.))),
        "Zero dayOfYear doesn't produce NULL");

    assertEquals(LocalDate.of(1999, 3, 1), makedate(1999., 60.),
        "Got Feb 29th of a non-lear year");
    assertEquals(LocalDate.of(1999, 12, 31), makedate(1999., 365.));
    assertEquals(LocalDate.of(2004, 12, 31), makedate(2004., 366.));
  }

  @Test
  public void checkRounding() {
    assertEquals(LocalDate.of(42, 1, 1), makedate(42.49, 1.49));
    assertEquals(LocalDate.of(43, 1, 2), makedate(42.50, 1.50));
  }

  @Test
  public void checkNullValues() {
    when(nullRef.valueOf(env)).thenReturn(nullValue());

    assertEquals(nullValue(), eval(makedate(nullRef, DSL.literal(42.))));
    assertEquals(nullValue(), eval(makedate(DSL.literal(42.), nullRef)));
    assertEquals(nullValue(), eval(makedate(nullRef, nullRef)));
  }

  @Test
  public void checkMissingValues() {
    when(missingRef.valueOf(env)).thenReturn(missingValue());

    assertEquals(missingValue(), eval(makedate(missingRef, DSL.literal(42.))));
    assertEquals(missingValue(), eval(makedate(DSL.literal(42.), missingRef)));
    assertEquals(missingValue(), eval(makedate(missingRef, missingRef)));
  }

  @Test
  public void checkRandomValues() {
    var r = new Random();
    for (var ignored : IntStream.range(0, 125).toArray()) {
      // Avoid zero values
      var year = r.nextDouble() * 5000 + 1;
      var dayOfYear = r.nextDouble() * 1000 + 1;

      LocalDate actual = makedate(year, dayOfYear);
      LocalDate expected = getReferenceValue(year, dayOfYear);

      assertEquals(expected, actual,
          String.format("year = %f, dayOfYear = %f", year, dayOfYear));
    }
  }

  /**
   * Using another algorithm to get reference value.
   * We should go to the next year until remaining @dayOfYear is bigger than 365/366.
   * @param year Year.
   * @param dayOfYear Day of the year.
   * @return The calculated date.
   */
  private LocalDate getReferenceValue(double year, double dayOfYear) {
    var yearL = (int)Math.round(year);
    var dayL = (int)Math.round(dayOfYear);
    while (true) {
      int daysInYear = Year.isLeap(yearL) ? 366 : 365;
      if (dayL > daysInYear) {
        dayL -= daysInYear;
        yearL++;
      } else {
        break;
      }
    }
    return LocalDate.ofYearDay(yearL, dayL);
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }
}
