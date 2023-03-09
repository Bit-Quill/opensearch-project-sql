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

/**
 * This visitor's sole purpose is to throw UnsupportedOperationExceptions
 * for unsupported features in the new engine when JSON format is specified.
 * Unsupported features in V2 are ones the produce results that differ from
 * legacy results.
 */
public class JsonSupportVisitor extends AbstractNodeVisitor<Boolean, JsonSupportVisitorContext> {
  @Override
  public Boolean visit(Node node, JsonSupportVisitorContext context) {
    return visitChildren(node, context);
  }

  @Override
  public Boolean visitChildren(Node node, JsonSupportVisitorContext context) {
    for (Node child : node.getChild()) {
      if (!child.accept(this, context)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean visitAggregation(Aggregation node, JsonSupportVisitorContext context) {
    if (node.getGroupExprList().isEmpty()) {
      return Boolean.TRUE;
    } else {
      context.setUnsupportedOperationException(new UnsupportedOperationException(
          "Queries with aggregation are not yet supported with json format in the new engine"));
      return Boolean.FALSE;
    }
  }

  @Override
  public Boolean visitFilter(Filter node, JsonSupportVisitorContext context) {
    return visit(node, context)
        && node.getCondition().accept(this, context);
  }

  @Override
  public Boolean visitFunction(Function node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (!context.isVisitingProject()) {
      return Boolean.TRUE;
    }

    // queries with function calls are not supported.
    context.setUnsupportedOperationException(new UnsupportedOperationException(
        "Queries with functions are not yet supported with json format in the new engine"));
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitLiteral(Literal node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (!context.isVisitingProject()) {
      return Boolean.TRUE;
    }

    // queries with literal values are not supported
    context.setUnsupportedOperationException(new UnsupportedOperationException(
        "Queries with literals are not yet supported with json format in the new engine"));
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitCast(Cast node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (!context.isVisitingProject()) {
      return Boolean.TRUE;
    }

    // Queries with cast are not supported
    context.setUnsupportedOperationException(new UnsupportedOperationException(
        "Queries with casts are not yet supported with json format in the new engine"));
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitAlias(Alias node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (!context.isVisitingProject()) {
      return Boolean.TRUE;
    }

    // Alias node is accepted if it does not have a user-defined alias
    // and if the delegated expression is accepted.
    if (StringUtils.isEmpty(node.getAlias())) {
      return node.getDelegated().accept(this, context);
    } else {
      context.setUnsupportedOperationException(new UnsupportedOperationException(
          "Queries with aliases are not yet supported with json format in the new engine"));
      return Boolean.FALSE;
    }
  }

  @Override
  public Boolean visitProject(Project node, JsonSupportVisitorContext context) {
    boolean isSupported = visit(node, context);

    context.setVisitingProject(true);
    isSupported = isSupported ? node.getProjectList().stream()
        .allMatch(e -> e.accept(this, context)) : Boolean.FALSE;
    context.setVisitingProject(false);

    return isSupported;
  }
}
