/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;


/**
 * Expression node of MatchQueryAltSyntax function.
 */
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class MatchQueryAltSyntaxFunction extends UnresolvedExpression {
  @Getter
  private UnresolvedExpression stringPatternExpr;
  @Getter
  private UnresolvedExpression searchStringExpr;

  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(stringPatternExpr, searchStringExpr);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitMatchQueryAltSyntaxFunction(this, context);
  }
}