/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Expression node that includes a RelevanceField nodes.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
public class RelevanceField extends UnresolvedExpression {
  @Getter
  private UnresolvedExpression fieldName;
  @Getter
  private UnresolvedExpression fieldWeight;

  @Override
  public List<UnresolvedExpression> getChild() {
    return List.of(fieldName, fieldWeight);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitRelevanceField(this, context);
  }

  @Override
  public String toString() {
    return String.format("\"%s\" ^ %s", fieldName.toString(), fieldWeight.toString());
  }
}
