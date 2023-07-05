/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;


import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.ast.AbstractNodeVisitor;

@Getter
@EqualsAndHashCode(callSuper = false)
public class ArrayQualifiedName extends QualifiedName {

  private int index;

  public ArrayQualifiedName(String name, int index) {
    super(name);
    this.index = index;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitArrayQualifiedName(this, context);
  }
}
