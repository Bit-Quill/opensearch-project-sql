/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.NestedFunction;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.planner.logical.LogicalPlan;

/**
 * Analyze nested in the {@link AnalysisContext} to construct the {@link
 * LogicalPlan}.
 */
@RequiredArgsConstructor
public class NestedAnalyzer extends AbstractNodeVisitor<LogicalPlan, AnalysisContext> {
  private final ExpressionAnalyzer expressionAnalyzer;
//  private final LogicalPlan child;

  public LogicalPlan analyze(UnresolvedExpression projectItem, AnalysisContext context) {
    LogicalPlan nested = projectItem.accept(this, context);
    return nested;
  }

  @Override
  public LogicalPlan visitNestedFunction(NestedFunction node, AnalysisContext context) {
//    UnresolvedExpression delegated = node.getDelegated();
//    if (!(delegated instanceof NestedFunction)) {
//      return null;
//    }
//
//    NestedFunction unresolved = (NestedFunction) delegated;
//    Expression field = expressionAnalyzer.analyze(unresolved.getNestedField(), context);
//    return new LogicalNested(field);
    return null;
  }
}
