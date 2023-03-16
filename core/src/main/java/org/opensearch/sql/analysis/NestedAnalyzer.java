/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPlan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

/**
 * Analyze the Nested Function in the {@link AnalysisContext} to construct the {@link
 * LogicalPlan}.
 */
@RequiredArgsConstructor
public class NestedAnalyzer extends AbstractNodeVisitor<LogicalPlan, AnalysisContext> {
  private final ExpressionAnalyzer expressionAnalyzer;
  private final LogicalPlan child;

  public LogicalPlan analyze(UnresolvedExpression projectItem, AnalysisContext context) {
    LogicalPlan nested = projectItem.accept(this, context);
    return (nested == null) ? child : nested;
  }

  @Override
  public LogicalPlan visitAlias(Alias node, AnalysisContext context) {
    Map<String, ReferenceExpression> args = new HashMap<>();

    if (node.getDelegated() instanceof Function &&
        ((Function) node.getDelegated()).getFuncName().equalsIgnoreCase("nested")) {

      List<UnresolvedExpression> expressions = ((Function) node.getDelegated()).getFuncArgs();
      ReferenceExpression nestedField = (ReferenceExpression)expressionAnalyzer.analyze(expressions.get(0), context);
      if (expressions.size() == 2) {
        args.put("field", nestedField);
        args.put("path", (ReferenceExpression)expressionAnalyzer.analyze(expressions.get(1), context));
      } else {
        args.put("field", (ReferenceExpression)expressionAnalyzer.analyze(expressions.get(0), context));
        args.put("path", generatePath(nestedField.toString()));
      }
      return new LogicalNested(child, List.of(args), null);

    } else if (node.getDelegated() instanceof QualifiedName) {

//      return new LogicalNested(child, null,
//          List.of((ReferenceExpression)expressionAnalyzer.analyze(node.getDelegated(), context))
//      );
      return null;
    }
    return null;
  }

  private ReferenceExpression generatePath(String field) {
    return new ReferenceExpression(field.substring(0, field.lastIndexOf(".")), STRING);
  }
}
