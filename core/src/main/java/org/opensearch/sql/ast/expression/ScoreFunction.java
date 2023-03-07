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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Expression node of Highlight function.
 */
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class ScoreFunction extends UnresolvedExpression {
  private final UnresolvedExpression relevanceQuery;
  private final List<UnresolvedExpression> funcArgs;

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitScoreFunction(this, context);
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    List<UnresolvedExpression> resultingList = List.of(relevanceQuery);
    resultingList.addAll(funcArgs);
    return resultingList;
  }
}
