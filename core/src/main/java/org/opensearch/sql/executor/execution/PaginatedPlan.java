/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import org.opensearch.sql.ast.tree.Paginate;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;

public class PaginatedPlan extends AbstractPlan {
  final UnresolvedPlan plan;
  final int fetchSize;
  final PaginatedQueryService queryService;
  final ResponseListener<ExecutionEngine.QueryResponse>
      queryResponseResponseListener;

  /**
   * Create an abstract plan that can start paging a query.
   */
  public PaginatedPlan(QueryId queryId, UnresolvedPlan plan, int fetchSize,
                       PaginatedQueryService queryService,
                       ResponseListener<ExecutionEngine.QueryResponse>
                           queryResponseResponseListener) {
    super(queryId);
    this.plan = plan;
    this.fetchSize = fetchSize;
    this.queryService = queryService;
    this.queryResponseResponseListener = queryResponseResponseListener;
  }

  @Override
  public void execute() {
    queryService.execute(new Paginate(fetchSize, plan), queryResponseResponseListener);
  }

  @Override
  public void explain(ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    listener.onFailure(new UnsupportedOperationException(
        "`explain` feature for paginated requests is not implemented yet."));
  }
}
