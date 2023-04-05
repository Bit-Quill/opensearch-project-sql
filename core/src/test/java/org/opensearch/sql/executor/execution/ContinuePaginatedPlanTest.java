/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.DefaultExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.pagination.PaginatedPlanCache;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class ContinuePaginatedPlanTest {

  private static PaginatedPlanCache paginatedPlanCache;

  private static QueryService queryService;

  /**
   * Initialize the mocks.
   */
  @BeforeAll
  public static void setUp() {
    var storageEngine = mock(StorageEngine.class);
    paginatedPlanCache = new PaginatedPlanCache(storageEngine);
    queryService = new QueryService(null, new DefaultExecutionEngine(), null, null);
  }

  @Test
  public void can_execute_plan() {
    var planCache = mock(PaginatedPlanCache.class);
    when(planCache.convertToPlan(anyString())).thenReturn(mock(PhysicalPlan.class));
    var listener = new ResponseListener<ExecutionEngine.QueryResponse>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        assertNotNull(response);
      }

      @Override
      public void onFailure(Exception e) {
        fail();
      }
    };
    var plan = new ContinuePaginatedPlan(QueryId.None, "", queryService, planCache, listener);
    plan.execute();
  }

  @Test
  public void can_handle_error_while_executing_plan() {
    var listener = new ResponseListener<ExecutionEngine.QueryResponse>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        fail();
      }

      @Override
      public void onFailure(Exception e) {
        assertNotNull(e);
      }
    };
    var plan = new ContinuePaginatedPlan(QueryId.None, "", queryService,
        paginatedPlanCache, listener);
    plan.execute();
  }

  @Test
  public void explain_is_not_supported() {
    var listener = mock(ResponseListener.class);
    mock(ContinuePaginatedPlan.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))
        .explain(listener);
    verify(listener).onFailure(any(UnsupportedOperationException.class));
  }
}
