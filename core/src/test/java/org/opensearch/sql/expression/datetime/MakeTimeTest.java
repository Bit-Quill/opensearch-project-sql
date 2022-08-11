/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.missingValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
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
public class MakeTimeTest extends ExpressionTestBase {

  @Mock
  Environment<Expression, ExprValue> env;

  @Mock
  Expression nullRef;

  @Mock
  Expression missingRef;

  private FunctionExpression maketime(Expression hour, Expression minute, Expression second) {
    var repo = new ExpressionConfig().functionRepository();
    var func = repo.resolve(new FunctionSignature(new FunctionName("maketime"),
        List.of(DOUBLE, DOUBLE, DOUBLE)));
    return (FunctionExpression)func.apply(List.of(hour, minute, second));
  }

  private LocalTime maketime(Double hour, Double minute, Double second) {
    return maketime(DSL.literal(hour), DSL.literal(minute), DSL.literal(second))
        .valueOf(null).timeValue();
  }

  @Test
  public void checkEdgeCases() {
    assertEquals(nullValue(), eval(maketime(DSL.literal(-1.), DSL.literal(42.), DSL.literal(42.))),
        "Negative hour doesn't produce NULL");
    assertEquals(nullValue(), eval(maketime(DSL.literal(42.), DSL.literal(-1.), DSL.literal(42.))),
        "Negative minute doesn't produce NULL");
    assertEquals(nullValue(), eval(maketime(DSL.literal(12.), DSL.literal(42.), DSL.literal(-1.))),
        "Negative second doesn't produce NULL");

    assertThrows(DateTimeParseException.class,
        () -> eval(maketime(DSL.literal(24.), DSL.literal(42.), DSL.literal(42.))));
    assertThrows(DateTimeParseException.class,
        () -> eval(maketime(DSL.literal(12.), DSL.literal(60.), DSL.literal(42.))));
    assertThrows(DateTimeParseException.class,
        () -> eval(maketime(DSL.literal(12.), DSL.literal(42.), DSL.literal(60.))));

    assertEquals(LocalTime.of(23, 59, 59), maketime(23., 59., 59.));
    assertEquals(LocalTime.of(0, 0, 0), maketime(0., 0., 0.));
  }

  @Test
  public void checkRounding() {
    assertEquals(LocalTime.of(0, 0, 0), maketime(0.49, 0.49, 0.));
    assertEquals(LocalTime.of(1, 1, 0), maketime(0.50, 0.50, 0.));
  }

  @Test
  public void checkSecondFraction() {
    assertEquals(LocalTime.of(0, 0, 0).withNano(999999999), maketime(0., 0., 0.999999999));
    assertEquals(LocalTime.of(0, 0, 0).withNano(100502000), maketime(0., 0., 0.100502));
  }

  @Test
  public void checkNullValues() {
    when(nullRef.valueOf(env)).thenReturn(nullValue());

    assertEquals(nullValue(), eval(maketime(nullRef, DSL.literal(42.), DSL.literal(42.))));
    assertEquals(nullValue(), eval(maketime(DSL.literal(42.), nullRef, DSL.literal(42.))));
    assertEquals(nullValue(), eval(maketime(DSL.literal(42.), DSL.literal(42.), nullRef)));
    assertEquals(nullValue(), eval(maketime(nullRef, nullRef, DSL.literal(42.))));
    assertEquals(nullValue(), eval(maketime(nullRef, DSL.literal(42.), nullRef)));
    assertEquals(nullValue(), eval(maketime(nullRef, nullRef, nullRef)));
    assertEquals(nullValue(), eval(maketime(DSL.literal(42.), nullRef, nullRef)));
  }

  @Test
  public void checkMissingValues() {
    when(missingRef.valueOf(env)).thenReturn(missingValue());

    assertEquals(missingValue(), eval(maketime(missingRef, DSL.literal(42.), DSL.literal(42.))));
    assertEquals(missingValue(), eval(maketime(DSL.literal(42.), missingRef, DSL.literal(42.))));
    assertEquals(missingValue(), eval(maketime(DSL.literal(42.), DSL.literal(42.), missingRef)));
    assertEquals(missingValue(), eval(maketime(missingRef, missingRef, DSL.literal(42.))));
    assertEquals(missingValue(), eval(maketime(missingRef, DSL.literal(42.), missingRef)));
    assertEquals(missingValue(), eval(maketime(missingRef, missingRef, missingRef)));
    assertEquals(missingValue(), eval(maketime(DSL.literal(42.), missingRef, missingRef)));
  }

  @Test
  public void checkRandomValues() {
    var r = new Random();
    assertEquals(maketime(20., 30., 40.), LocalTime.of(20, 30, 40));

    for (var ignored : IntStream.range(0, 125).toArray()) {
      var hour = r.nextDouble() * 23;
      var minute = r.nextDouble() * 59;
      var second = r.nextDouble() * 59;
      // results could have 1 nanosec diff because of rounding FP
      var expected = LocalTime.of((int)Math.round(hour), (int)Math.round(minute),
          // pick fraction second part as nanos
          (int)Math.floor(second)).withNano((int)((second % 1) * 1E9));
      var delta = Duration.between(expected, maketime(hour, minute, second)).getNano();
      assertEquals(0, delta, 1,
          String.format("hour = %f, minute = %f, second = %f", hour, minute, second));
    }
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }
}
