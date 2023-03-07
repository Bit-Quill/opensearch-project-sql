/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import lombok.Getter;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Score Expression.
 */
@Getter
public class ScoreExpression extends FunctionExpression {

  private final Expression relevanceQueryExpr;

  /**
   * ScoreExpression Constructor.
   * @param relevanceQueryExpr : relevanceQueryExpr for expression.
   */
  public ScoreExpression(Expression relevanceQueryExpr) {
    super(BuiltinFunctionName.SCORE.getName(), List.of(relevanceQueryExpr));
    this.relevanceQueryExpr = relevanceQueryExpr;
  }

  /**
   * Return collection value matching relevance query expression.
   * @param valueEnv : Dataset to parse value from.
   * @return : collection value of relevance query expression.
   */
  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
//    String refName = "_highlight";
//    // Not a wilcard expression
//    if (this.type == ExprCoreType.ARRAY) {
//      refName += "." + StringUtils.unquoteText(getHighlightField().toString());
//    }
//    ExprValue value = valueEnv.resolve(DSL.ref(refName, ExprCoreType.STRING));
//
//    // In the event of multiple returned highlights and wildcard being
//    // used in conjunction with other highlight calls, we need to ensure
//    // only wildcard regex matching is mapped to wildcard call.
//    if (this.type == ExprCoreType.STRUCT && value.type() == ExprCoreType.STRUCT) {
//      value = new ExprTupleValue(
//          new LinkedHashMap<String, ExprValue>(value.tupleValue()
//              .entrySet()
//              .stream()
//              .filter(s -> matchesHighlightRegex(s.getKey(),
//                  StringUtils.unquoteText(highlightField.toString())))
//              .collect(Collectors.toMap(
//                  e -> e.getKey(),
//                  e -> e.getValue()))));
//      if (value.tupleValue().isEmpty()) {
//        value = ExprValueUtils.missingValue();
//      }
//    }

    // TODO: this is where we visit relevance function nodes and update BOOST values as necessary
    // Otherwise, this is a no-op

    return ExprNullValue.of();
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitScore(this, context);
  }

  @Override
  public ExprType type() {
    return ExprCoreType.UNDEFINED;
  }
}
