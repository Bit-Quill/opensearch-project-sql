/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.executor.PaginatedPlanCacheTest.buildCursor;

import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.DefaultExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.PaginatedPlanCache;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.TableScanOperator;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class ContinuePaginatedPlanTest {

  private static PaginatedPlanCache paginatedPlanCache;

  private static PaginatedQueryService paginatedQueryService;

  /**
   * Initialize the mocks.
   */
  @BeforeAll
  public static void setUp() {
    var storageEngine = mock(StorageEngine.class);
    when(storageEngine.getTableScan(anyString(), anyString()))
        .thenReturn(mock(TableScanOperator.class));
    paginatedPlanCache = new PaginatedPlanCache(storageEngine);
    paginatedQueryService = new PaginatedQueryService(
        null, new DefaultExecutionEngine(), null);
  }

  @Test
  public void none_plan_is_empty() {
    var plan = ContinuePaginatedPlan.None;

    assertAll(
        () -> assertTrue(plan.getQueryId().getQueryId().isEmpty()),
        () -> {
          var cursor = (String) FieldUtils.readField(plan, "cursor", true);
          assertTrue(cursor.isEmpty());
        },
        () -> {
          var pqs = (PaginatedQueryService) FieldUtils.readField(plan, "queryService", true);
          assertNull(pqs);
        },
        () -> {
          var ppc = (PaginatedPlanCache) FieldUtils.readField(plan, "paginatedPlanCache", true);
          assertNull(ppc);
        },
        () -> {
          var rl = (ResponseListener<?>) FieldUtils.readField(plan, "queryResponseListener", true);
          assertNull(rl);
        }
    );
  }

  @Test
  public void can_execute_plan() {
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
    var plan = new ContinuePaginatedPlan(QueryId.None, buildCursor(Map.of()),
        paginatedQueryService, paginatedPlanCache, listener);
    plan.execute();
  }

  @Test
  // Same as previous test, but with malformed cursor
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
    var plan = new ContinuePaginatedPlan(QueryId.None, buildCursor(Map.of("pageSize", "abc")),
        paginatedQueryService, paginatedPlanCache, listener);
    plan.execute();
  }

  @Test
  public void explain_is_not_supported() {
    assertThrows(UnsupportedOperationException.class,
        () -> ContinuePaginatedPlan.None.explain(null));
  }
}
