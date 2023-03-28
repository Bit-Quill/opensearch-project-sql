/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/**
 * Represent the All fields which is been used in SELECT *.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
public class NestedAllFields extends UnresolvedExpression {
  @Getter
  private final String path;

  public NestedAllFields(String path) {
    this.path = path.split("[.][*]")[0];
  }

  @Override
  public List<? extends Node> getChild() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitNestedAllFields(this, context);
  }
}
