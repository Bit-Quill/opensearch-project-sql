/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.tree.Paginate;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * `PaginatedQueryService` does the same as `QueryService`, but it has another planner,
 * configured to handle paged index scan.
 * @see OpenSearchPluginModule#queryPlanFactory (:plugin module)
 * @see LogicalPlanOptimizer#paginationCreate
 * @see QueryService
 */
@RequiredArgsConstructor
public class PaginatedQueryService {
  private final Analyzer analyzer;

  private final ExecutionEngine executionEngine;

  private final Planner planner;

  /**
   * Execute a pagination request. Passes the exception the listener.
   */
  public void execute(Paginate plan, ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      executePlan(analyze(plan), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  public void executePlan(LogicalPlan plan,
                          ResponseListener<ExecutionEngine.QueryResponse> listener) {
    executionEngine.execute(plan(plan), ExecutionContext.emptyExecutionContext(), listener);
  }

  /**
   * Execute a physical plan without analyzing or planning anything.
   */
  public void executePlan(PhysicalPlan plan,
                          ResponseListener<ExecutionEngine.QueryResponse> listener) {
    executionEngine.execute(plan, ExecutionContext.emptyExecutionContext(), listener);
  }

  public LogicalPlan analyze(UnresolvedPlan plan) {
    return analyzer.analyze(plan, new AnalysisContext());
  }

  public PhysicalPlan plan(LogicalPlan plan) {
    return planner.plan(plan);
  }
}
