/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
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
      String sExpression = cursor.substring(CURSOR_PREFIX.length());

      // TODO Parse sExpression and initialize variables below.
      // storageEngine needs to create the TableScanOperator.
      int pageSize = -1;
      int currentPageIndex = -1;
      List<NamedExpression> projectList = List.of();
      TableScanOperator scan = null;

      return new PaginateOperator(new ProjectOperator(scan, projectList, List.of()),
          pageSize, currentPageIndex);

    } else {
      throw new UnsupportedOperationException("Unsupported cursor");
    }
  }
}
