/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * Use this unresolved plan visitor to check if a plan can be serialized by PaginatedPlanCache.
 * If plan.accept(new CanpaginateVisitor(...)) returns true,
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

  @Override
  public Boolean visit(Node node, Object context) {
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitProject(Project node, Object context) {
    var projections = node.getProjectList();
    if (projections.size() != 1) {
      return Boolean.FALSE;
    }

    if (!(projections.get(0) instanceof AllFields)) {
      return Boolean.FALSE;
    }

    var children = node.getChild();
    if (children.size() != 1) {
      return Boolean.FALSE;
    }

    return children.get(0).accept(this, context);
  }
}
