/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import java.util.List;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.ast.AbstractNodeVisitor;


/**
 * Expression node that holds a function which should be replaced by its constant[1] value.
 * [1] Constant at execution time.
 */
@EqualsAndHashCode(callSuper = false)
public class FunctionLikeConstant extends Function {

  public FunctionLikeConstant(String funcName, List<UnresolvedExpression> funcArgs) {
      super(funcName, funcArgs);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitFunctionLikeConstant(this, context);
  }
}
