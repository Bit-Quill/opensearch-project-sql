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
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Project;

public class JsonSupportAnalyzer extends AbstractNodeVisitor<Boolean, Object> {
  @Override
  public Boolean visit(Node node, Object context) {
    // A node is supported if all of its children are supported.
    return node.getChild().stream().allMatch(c -> c.accept(this, null));
  }

  @Override
  public Boolean visitChildren(Node node, Object context) {
    for (Node child : node.getChild()) {
      if (!child.accept(this, null))
        return false;
    }
    return true;
  }

  @Override
  public Boolean visitFunction(Function node, Object context) {
    // queries with function calls are not supported.
    return false;
  }

  @Override
  public Boolean visitLiteral(Literal node, Object context) {
    // queries with literal values are not supported
    return false;
  }

  @Override
  public Boolean visitCast(Cast node, Object context) {
    // Queries with cast are not supported
    return false;
  }

  @Override
  public Boolean visitAlias(Alias node, Object context) {
    // Alias node is accepted if it does not have a user-defined alias
    // and if the delegated expression is accepted.
    if (!StringUtils.isEmpty(node.getAlias()))
      return false;
    else {
      return node.getDelegated().accept(this, null);
    }
  }

  @Override
  public Boolean visitProject(Project node, Object context) {
    return visit(node, null)
        && node.getProjectList().stream().allMatch(e -> e.accept(this, null));
  }

  @Override
  public Boolean visitAggregation(Aggregation node, Object context) {
    return node.getGroupExprList().isEmpty();
  }

  @Override
  public Boolean visitFilter(Filter node, Object context) {
    return visit(node, null)
        && node.getCondition().accept(this, null);
  }
}
