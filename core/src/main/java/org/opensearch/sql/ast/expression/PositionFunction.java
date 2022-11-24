/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

/**
 * Expression node of Highlight function.
 */
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class PositionFunction extends UnresolvedExpression {
  @Getter
  private UnresolvedExpression left;
  @Getter
  private UnresolvedExpression right;

  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(left, right);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitPositionFunction(this, context);
  }
}
