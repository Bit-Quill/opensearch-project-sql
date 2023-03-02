/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Project;

public class JsonSupportAnalyzer extends AbstractNodeVisitor<Boolean, JsonSupportAnalysisContext> {
  @Override
  public Boolean visit(Node node, JsonSupportAnalysisContext context) {
    // A node is supported if all of its children are supported.
    return node.getChild().stream().filter(c -> c.accept(this, context) != null)
        .allMatch(c -> c.accept(this, context));
  }

  @Override
  public Boolean visitChildren(Node node, JsonSupportAnalysisContext context) {
    for (Node child : node.getChild()) {
      if (!child.accept(this, context))
        return false;
    }
    return true;
  }

  @Override
  public Boolean visitFunction(Function node, JsonSupportAnalysisContext context) {
    // queries with function calls are not supported.
    return false;
  }

  @Override
  public Boolean visitLiteral(Literal node, JsonSupportAnalysisContext context) {
    // queries with literal values are not supported
    return false;
  }

  @Override
  public Boolean visitCast(Cast node, JsonSupportAnalysisContext context) {
    // Queries with cast are not supported
    return false;
  }

  @Override
  public Boolean visitAlias(Alias node, JsonSupportAnalysisContext context) {
    // Alias node is accepted if it does not have a user-defined alias
    // and if the delegated expression is accepted.
    if (!StringUtils.isEmpty(node.getAlias()))
      return false;
    else {
      return node.getDelegated().accept(this, context);
    }
  }

  @Override
  public Boolean visitProject(Project node, JsonSupportAnalysisContext context) {
    return visit(node, context)
        && node.getProjectList().stream().filter(c -> c.accept(this, context) != null)
        .allMatch(e -> e.accept(this, context));
  }

  @Override
  public Boolean visitAggregation(Aggregation node, JsonSupportAnalysisContext context) {
    return node.getGroupExprList().isEmpty();
  }
}
