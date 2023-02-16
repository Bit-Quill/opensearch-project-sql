/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Function;

/**
 * AST node represent Unnest operation.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Unnest extends UnresolvedPlan {
  private final Function nested;
  private UnresolvedPlan child;

  public Unnest(Function nested) {
    this.nested = nested;
  }

  @Override
  public Unnest attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitUnnest(this, context);
  }
}
