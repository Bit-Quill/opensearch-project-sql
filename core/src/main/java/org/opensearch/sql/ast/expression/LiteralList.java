/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Expression node that includes a list of Expression nodes.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
public class LiteralList extends UnresolvedExpression {
  @Getter
  private List<UnresolvedExpression> literalList;

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.copyOf(literalList);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitLiteralList(this, context);
  }

  @Override
  public String toString() {
    return literalList.stream().map(String::valueOf).collect(Collectors.joining(", "));
  }
}
