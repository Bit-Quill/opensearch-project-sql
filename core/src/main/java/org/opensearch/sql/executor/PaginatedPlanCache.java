/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.opensearch.executor.Cursor;
import org.opensearch.sql.planner.PaginateOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

@RequiredArgsConstructor
public class PaginatedPlanCache {
  public static final String CURSOR_PREFIX = "n:";
  private final StorageEngine storageEngine;
  public static final PaginatedPlanCache None = new PaginatedPlanCache(null);

  public boolean canConvertToCursor(UnresolvedPlan plan) {
    return plan.accept(new CanPaginateVisitor(), null);
  }

  @RequiredArgsConstructor
  @Data
  static class SeriazationContext {
    private final PaginatedPlanCache cache;
  }

  /**
   * Converts a physical plan tree to a cursor. May cache plan related data somewhere.
   */
  public Cursor convertToCursor(PhysicalPlan plan) {
    if (plan instanceof PaginateOperator) {
      var raw = CURSOR_PREFIX + plan.toCursor();
      return new Cursor(raw.getBytes());
    } else {
      return Cursor.None;
    }
  }

  /**
    * Converts a cursor to a physical plan tree.
    */
  public PhysicalPlan convertToPlan(String cursor) {
    if (cursor.startsWith(CURSOR_PREFIX)) {
      try {
        String expression = cursor.substring(CURSOR_PREFIX.length());

        // TODO Parse expression and initialize variables below.
        // storageEngine needs to create the TableScanOperator.

        // TODO Parse with ANTLR or serialize as JSON/XML
        if (!expression.startsWith("(Paginate,")) {
          throw new UnsupportedOperationException("Unsupported cursor");
        }
        expression = expression.substring(expression.indexOf(',') + 1);
        int currentPageIndex = Integer.parseInt(expression, 0, expression.indexOf(','), 10);

        expression = expression.substring(expression.indexOf(',') + 1);
        int pageSize = Integer.parseInt(expression, 0, expression.indexOf(','), 10);

        expression = expression.substring(expression.indexOf(',') + 1);
        if (!expression.startsWith("(Project,")) {
          throw new UnsupportedOperationException("Unsupported cursor");
        }
        expression = expression.substring(expression.indexOf(',') + 1);
        if (!expression.startsWith("(namedParseExpressions,")) {
          throw new UnsupportedOperationException("Unsupported cursor");
        }
        expression = expression.substring(expression.indexOf(',') + 1);
        var serializer = new DefaultExpressionSerializer();
        // TODO parse npe
        List<NamedExpression> namedParseExpressions = List.of();

        expression = expression.substring(expression.indexOf(',') + 1);
        List<NamedExpression> projectList = new ArrayList<>();
        if (!expression.startsWith("(projectList,")) {
          throw new UnsupportedOperationException("Unsupported cursor");
        }
        expression = expression.substring(expression.indexOf(',') + 1);
        while (expression.startsWith("(named,")) {
          expression = expression.substring(expression.indexOf(',') + 1);
          var name = expression.substring(0, expression.indexOf(','));
          expression = expression.substring(expression.indexOf(',') + 1);
          var alias = expression.substring(0, expression.indexOf(','));
          if (alias.isEmpty()) {
            alias = null;
          }
          expression = expression.substring(expression.indexOf(',') + 1);
          projectList.add(new NamedExpression(name,
              serializer.deserialize(expression.substring(0, expression.indexOf(')'))), alias));
          expression = expression.substring(expression.indexOf(',') + 1);
        }

        if (!expression.startsWith("(OpenSearchPagedIndexScan,")) {
          throw new UnsupportedOperationException("Unsupported cursor");
        }
        expression = expression.substring(expression.indexOf(',') + 1);
        var indexName = expression.substring(0, expression.indexOf(','));
        expression = expression.substring(expression.indexOf(',') + 1);
        var scrollId = expression.substring(0, expression.indexOf(')'));
        TableScanOperator scan = storageEngine.getTableScan(indexName, scrollId);

        return new PaginateOperator(new ProjectOperator(scan, projectList, namedParseExpressions),
            pageSize, currentPageIndex);
      } catch (Exception e) {
        throw new UnsupportedOperationException("Unsupported cursor", e);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported cursor");
    }
  }
}
