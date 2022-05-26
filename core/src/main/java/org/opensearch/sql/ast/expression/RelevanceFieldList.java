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
 * Expression node that includes a list of RelevanceField nodes.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
public class RelevanceFieldList extends UnresolvedExpression {
  @Getter
  private java.util.Map<UnresolvedExpression, UnresolvedExpression> fieldList;

  @Override
  public List<UnresolvedExpression> getChild() {
    return List.of();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitRelevanceFieldList(this, context);
  }

  @Override
  public String toString() {
    return fieldList
        .entrySet()
        .stream()
        .map(e -> String.format("\"%s\" ^ %s", e.getKey().toString(), e.getValue().toString()))
        .collect(Collectors.joining(", "));
  }
}
