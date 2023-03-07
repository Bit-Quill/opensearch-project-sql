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

public class JsonSupportVisitor extends AbstractNodeVisitor<Boolean, Object> {
  @Override
  public Boolean visit(Node node, Object context) {
    // A node is supported if all of its children are supported.
    return node.getChild().stream().allMatch(c -> c.accept(this, null));
  }

  @Override
  public Boolean visitChildren(Node node, Object context) {
    for (Node child : node.getChild()) {
      child.accept(this, null);
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean visitAggregation(Aggregation node, Object context) {
    if (node.getGroupExprList().isEmpty()) {
      return Boolean.TRUE;
    } else {
      throw new UnsupportedOperationException(
          "Queries with aggregation are not yet supported with json format in the new engine");
    }
  }

  @Override
  public Boolean visitFilter(Filter node, Object context) {
    return visit(node, null)
        && node.getCondition().accept(this, null);
  }

  @Override
  public Boolean visitProject(Project node, Object context) {
    // Overridden visit functions are done in memory and are not supported with json format
    class UnsupportedProjectVisitor
        extends AbstractNodeVisitor<Boolean, Object> {
      @Override
      public Boolean visit(Node node, Object context) {
        // A node is supported if all of its children are supported.
        return node.getChild().stream().allMatch(c -> c.accept(this, null));
      }

      @Override
      public Boolean visitChildren(Node node, Object context) {
        for (Node child : node.getChild()) {
          child.accept(this, null);
        }
        return Boolean.TRUE;
      }

      @Override
      public Boolean visitFunction(Function node, Object context) {
        // queries with function calls are not supported.
        throw new UnsupportedOperationException(
            "Queries with functions are not yet supported with json format in the new engine");
      }

      @Override
      public Boolean visitLiteral(Literal node, Object context) {
        // queries with literal values are not supported
        throw new UnsupportedOperationException(
            "Queries with literals are not yet supported with json format in the new engine");
      }

      @Override
      public Boolean visitCast(Cast node, Object context) {
        // Queries with cast are not supported
        throw new UnsupportedOperationException(
            "Queries with casts are not yet supported with json format in the new engine");
      }

      @Override
      public Boolean visitAlias(Alias node, Object context) {
        // Alias node is accepted if it does not have a user-defined alias
        // and if the delegated expression is accepted.
        if (StringUtils.isEmpty(node.getAlias())) {
          return node.getDelegated().accept(this, null);
        } else {
          throw new UnsupportedOperationException(
              "Queries with aliases are not yet supported with json format in the new engine");
        }
      }
    }

    return visit(node, null) && node.getProjectList().stream()
        .allMatch(e -> e.accept(new UnsupportedProjectVisitor(), context));
  }
}
