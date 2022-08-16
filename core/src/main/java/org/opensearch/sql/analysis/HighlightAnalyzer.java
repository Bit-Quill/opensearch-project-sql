/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalPlan;

import java.util.Map;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

/**
 * Analyze the highlight in the {@link AnalysisContext} to construct the {@link
 * LogicalPlan}.
 */
@RequiredArgsConstructor
public class HighlightAnalyzer extends AbstractNodeVisitor<LogicalPlan, AnalysisContext> {
  private final ExpressionAnalyzer expressionAnalyzer;
  private final LogicalPlan child;

  public LogicalPlan analyze(UnresolvedExpression projectItem, AnalysisContext context) {
    LogicalPlan highlight = projectItem.accept(this, context);
    if(highlight == null) {
      return child;
    } else {
//      TypeEnvironment typeEnvironment = context.peek();
//      var blah = "highlight(Body)";
//      typeEnvironment.define(new Symbol(Namespace.FIELD_NAME, blah), STRING);
      return highlight;
    }
//    return (highlight == null) ? child : highlight;
  }

  @Override
  public LogicalPlan visitAlias(Alias node, AnalysisContext context) {
    if (!(node.getDelegated() instanceof HighlightFunction)) {
      return null;
    }

    HighlightFunction unresolved = (HighlightFunction) node.getDelegated();
    Expression field = expressionAnalyzer.analyze(unresolved.getHighlightField(), context);
    return new LogicalHighlight(child, field);
  }

//  @Override
//  public LogicalPlan visitAllFields(AllFields node, AnalysisContext context) {
//
////    HighlightFunction unresolved = (HighlightFunction) node.getDelegated();
////    Expression field = expressionAnalyzer.analyze(((LogicalHighlight) this.child.getChild()).getHighlightField(), context);
//    Expression field = expressionAnalyzer.analyze(node.getHighlight(), context);
//
//    TypeEnvironment environment = context.peek();
//    Map<String, ExprType> lookupAllFields = environment.lookupAllFields(Namespace.FIELD_NAME);
//
//    return new LogicalHighlight(child, field);
//  }
}
