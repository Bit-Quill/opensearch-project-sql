/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;

public class DateTimeExpressionTestBase extends ExpressionTestBase {
  Environment<Expression, ExprValue> env;

  protected Long unixTimeStamp() {
    return dsl.unixTimeStampExpr().valueOf(null).longValue();
  }

  protected FunctionExpression unixTimeStampOf(Expression value) {
    var repo = new ExpressionConfig().functionRepository();
    var func = repo.resolve(new FunctionSignature(new FunctionName("unix_timestamp"),
        List.of(value.type())));
    return (FunctionExpression) func.apply(List.of(value));
  }

  protected Double unixTimeStampOf(LocalDateTime value) {
    return unixTimeStampOf(DSL.literal(new ExprDatetimeValue(value))).valueOf(null).doubleValue();
  }

  private Double unixTimeStampOf(Instant value) {
    return unixTimeStampOf(DSL.literal(new ExprTimestampValue(value))).valueOf(null).doubleValue();
  }

  protected LocalDateTime fromUnixTime(Long value) {
    return dsl.fromUnixTime(DSL.literal(value)).valueOf(null).datetimeValue();
  }

  protected LocalDateTime fromUnixTime(Double value) {
    return dsl.fromUnixTime(DSL.literal(value)).valueOf(null).datetimeValue();
  }

  protected ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }
}
