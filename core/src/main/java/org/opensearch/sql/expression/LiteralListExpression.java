/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;

/**
 * Literal List Expression.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class LiteralListExpression implements Expression {
  private final ExprCollectionValue exprValue;

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> env) {
    return exprValue;
  }

  @Override
  public ExprType type() {
    return exprValue.type();
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitLiteralList(this, context);
  }

  @Override
  public String toString() {
    return exprValue.toString();
  }
}
