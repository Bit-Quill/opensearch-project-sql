/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;


import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

@Getter
@EqualsAndHashCode(callSuper = false)
public class ArrayQualifiedName extends QualifiedName {

  private final List<Pair<String, OptionalInt>> partsAndIndexes;

  public ArrayQualifiedName(List<Pair<String, OptionalInt>> parts) {
    super(parts.stream().map(p -> p.getLeft()).collect(Collectors.toList()));
    this.partsAndIndexes = parts;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitArrayQualifiedName(this, context);
  }
}
