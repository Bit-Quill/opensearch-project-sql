/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Expression node of relevance function.
 */
@EqualsAndHashCode(callSuper = false)
public class RelevanceFunction extends Function {

  public enum FunctionType {
    SINGLE_FIELD_FUNCTION,
    MULTI_FIELD_FUNCTION,
    INVALID_RELEVANCE_FUNCTION
  }

  @Getter
  FunctionType type;

  public RelevanceFunction(String funcName, List<UnresolvedExpression> funcArgs,
      FunctionType type) {
    super(funcName, funcArgs);
    this.type = type;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitRelevanceFunction(this, context);
  }
}
