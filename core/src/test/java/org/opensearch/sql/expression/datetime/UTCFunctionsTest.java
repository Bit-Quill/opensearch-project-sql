/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;



@ExtendWith(MockitoExtension.class)
class UTCFunctionsTest extends ExpressionTestBase {
  int allowedSecondsOffset = 60;

  @Mock
  Environment<Expression, ExprValue> env;

  @Test
  public void utcTimeStamp() {
    FunctionExpression expr = dsl.utc_timestamp();
    LocalDateTime result = expr.valueOf(env).datetimeValue();
    assertEquals(TIMESTAMP, expr.type());

    LocalDateTime currentUTC = LocalDateTime.now(ZoneOffset.UTC);
    LocalDateTime lowerUTC = currentUTC.minus(allowedSecondsOffset, ChronoUnit.SECONDS);
    LocalDateTime upperUTC = currentUTC.minus(-allowedSecondsOffset, ChronoUnit.SECONDS);

    if (!lowerUTC.isBefore(result) || !upperUTC.isAfter(result)) {
      assertTrue(Boolean.parseBoolean("TimeStamp was not within range"));
    }
  }

  @Test
  public void utcTime() {
    FunctionExpression expr = dsl.utc_time();
    LocalTime result = expr.valueOf(env).timeValue();
    assertEquals(ExprCoreType.TIME, expr.type());

    LocalDateTime currentUTC = LocalDateTime.now(ZoneOffset.UTC);
    LocalTime lowerUTC = currentUTC.minus(allowedSecondsOffset, ChronoUnit.SECONDS).toLocalTime();
    LocalTime upperUTC = currentUTC.minus(-allowedSecondsOffset, ChronoUnit.SECONDS).toLocalTime();

    if (!lowerUTC.isBefore(result) || !upperUTC.isAfter(result)) {
      assertTrue(Boolean.parseBoolean("Time was not within range"));
    }
  }

  @Test
  public void utcDate() {
    FunctionExpression expr = dsl.utc_date();
    LocalDate result = expr.valueOf(env).dateValue();
    assertEquals(ExprCoreType.DATE, expr.type());

    LocalDateTime currentUTC = LocalDateTime.now(ZoneOffset.UTC);
    LocalDate lowerUTC = currentUTC.minus(allowedSecondsOffset, ChronoUnit.SECONDS).toLocalDate();
    LocalDate upperUTC = currentUTC.minus(-allowedSecondsOffset, ChronoUnit.SECONDS).toLocalDate();

    if (lowerUTC.isAfter(result) || upperUTC.isBefore(result)) {
      assertTrue(Boolean.parseBoolean("Time was not within range"));
    }
  }
}
