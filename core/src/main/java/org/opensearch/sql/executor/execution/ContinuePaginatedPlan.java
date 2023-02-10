/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.PaginatedPlanCache;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.planner.physical.PhysicalPlan;

public class ContinuePaginatedPlan extends AbstractPlan {

  public static final ContinuePaginatedPlan None
      = new ContinuePaginatedPlan(QueryId.None, "", null,
      null, null);
  private final String cursor;
  private final PaginatedQueryService queryService;
  private final PaginatedPlanCache paginatedPlanCache;

  private final ResponseListener<ExecutionEngine.QueryResponse> queryResponseListener;


  /**
   * Create an abstract plan that can continue paginating a given cursor.
   */
  public ContinuePaginatedPlan(QueryId queryId, String cursor, PaginatedQueryService queryService,
                               PaginatedPlanCache ppc,
                               ResponseListener<ExecutionEngine.QueryResponse>
                                   queryResponseListener) {
    super(queryId);
    this.cursor = cursor;
    this.paginatedPlanCache = ppc;
    this.queryService = queryService;
    this.queryResponseListener = queryResponseListener;
  }

  @Override
  public void execute() {
    try {
      PhysicalPlan plan = paginatedPlanCache.convertToPlan(cursor);
      queryService.executePlan(plan, queryResponseListener);
    } catch (Exception e) {
      queryResponseListener.onFailure(e);
    }
  }

  @Override
  public void explain(ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    throw new NotImplementedException("Explain of query continuation is not supported");
  }
}
