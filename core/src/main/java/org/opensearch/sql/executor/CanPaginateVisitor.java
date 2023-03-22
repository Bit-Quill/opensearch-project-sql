/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Values;

/**
 * Use this unresolved plan visitor to check if a plan can be serialized by PaginatedPlanCache.
 * If plan.accept(new CanPaginateVisitor(...)) returns true,
 * then PaginatedPlanCache.convertToCursor will succeed.
 * Otherwise, it will fail.
 * Currently, the conditions are:
 * - only projection of a relation is supported.
 * - projection only has * (a.k.a. allFields).
 * - Relation only scans one table
 * - The table is an open search index.
 * See PaginatedPlanCache.canConvertToCursor for usage.
 */
public class CanPaginateVisitor extends AbstractNodeVisitor<Boolean, Object> {

  @Override
  public Boolean visitRelation(Relation node, Object context) {
    if (!node.getChild().isEmpty()) {
      // Relation instance should never have a child, but check just in case.
      return Boolean.FALSE;
    }

    // TODO use storageEngine from the calling PaginatedPlanCache to determine if
    // node.getTableQualifiedName is provided by the storage engine. Return false if it's
    // not the case.
    return Boolean.TRUE;
  }

  private Boolean canPaginate(Node node, Object context) {
    AtomicBoolean result = new AtomicBoolean(true);
    var childList = node.getChild();
    if (childList != null) {
      childList.forEach(n -> result.set(result.get() && n.accept(this, context)));
    }
    return result.get();
  }

  //For queries without `FROM` clause.
  //Required to overload `toCursor` function in `ValuesOperator` and modify cursor parsing.
  @Override
  public Boolean visitValues(Values node, Object context) {
    return canPaginate(node, context);
  }

  //For queries with LIMIT clause:
  //Required to overload `toCursor` function in `LimitOperator` and modify cursor parsing.
  @Override
  public Boolean visitLimit(Limit node, Object context) {
    return canPaginate(node, context);
  }

  //For queries with ORDER BY clause:
  //Required to overload `toCursor` function in `SortOperator` and modify cursor parsing.
  @Override
  public Boolean visitSort(Sort node, Object context) {
    return canPaginate(node, context);
  }

  //For queries with WHERE clause:
  //Required to overload `toCursor` function in `FilterOperator` and modify cursor parsing.
  @Override
  public Boolean visitFilter(Filter node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitLiteral(Literal node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitField(Field node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitAlias(Alias node, Object context) {
    return canPaginate(node, context) && canPaginate(node.getDelegated(), context);
  }

  @Override
  public Boolean visitAllFields(AllFields node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitQualifiedName(QualifiedName node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitChildren(Node node, Object context) {
    return Boolean.FALSE;
  }

  @Override
  public Boolean visit(Node node, Object context) {
    // for all not listed (= unchecked) - false
    // TODO evaluate to return true or call `callPaginate`
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitWindowFunction(WindowFunction node, Object context) {
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitProject(Project node, Object context) {
    // Allow queries with 'SELECT *' only. Those restriction could be removed, but consider
    // in-memory aggregation performed by window function (see WindowOperator).
    // SELECT max(age) OVER (PARTITION BY city) ...
    AtomicBoolean result = new AtomicBoolean(true);
    node.getProjectList().forEach(n -> result.set(result.get() && n.accept(this, context)));
    if (!result.get()) {
      return Boolean.FALSE;
    }

    var children = node.getChild();
    if (children.size() != 1) {
      return Boolean.FALSE;
    }

    return children.get(0).accept(this, context);
  }
}
