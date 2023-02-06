/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.datasource.model.DataSourceMetadata.defaultOpenSearchDataSourceMetadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.DataSourceServiceImpl;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.PaginatedPlanCache;
import org.opensearch.sql.executor.execution.PaginatedQueryService;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchRestClient;
import org.opensearch.sql.opensearch.executor.Cursor;
import org.opensearch.sql.opensearch.storage.OpenSearchDataSourceFactory;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.util.InternalRestHighLevelClient;
import org.opensearch.sql.util.StandaloneModule;

public class StandalonePaginationIT extends SQLIntegTestCase {

  private PaginatedQueryService paginatedQueryService;

  private PaginatedPlanCache paginatedPlanCache;

  private OpenSearchClient client;

  @Override
  public void init() {
    RestHighLevelClient restClient = new InternalRestHighLevelClient(client());
    client = new OpenSearchRestClient(restClient);
    DataSourceService dataSourceService = new DataSourceServiceImpl(
        new ImmutableSet.Builder<DataSourceFactory>()
            .add(new OpenSearchDataSourceFactory(client, defaultSettings()))
            .build());
    dataSourceService.addDataSource(defaultOpenSearchDataSourceMetadata());

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(new StandaloneModule(new InternalRestHighLevelClient(client()), defaultSettings(), dataSourceService));
    Injector injector = modules.createInjector();

    paginatedQueryService = injector.getInstance(PaginatedQueryService.class);
    paginatedPlanCache = injector.getInstance(PaginatedPlanCache.class);
  }

  @Test
  public void testPagination() throws IOException {
    class TestResponder
        implements ResponseListener<ExecutionEngine.QueryResponse> {
      @Getter
      Cursor cursor = Cursor.None;
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        cursor = response.getCursor();
        assertTrue(true);
      }

      @Override
      public void onFailure(Exception e) {

        assertFalse(true);
      }
    };

    // arrange
    {
      Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
      request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
      client().performRequest(request1);
      Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
      request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
      client().performRequest(request2);
    }

    // act 1, asserts in firstResponder
    var t = new OpenSearchIndex(client, defaultSettings(), "test");
    LogicalPlan p = new LogicalPaginate(1, List.of(new LogicalProject(
        new LogicalRelation("test", t),
        List.of(new NamedExpression("count()", new LiteralExpression(ExprBooleanValue.of(true)))),
        List.of()
    )));
    var firstResponder = new TestResponder();
    paginatedQueryService.executePlan(p, firstResponder);

    // act 2, asserts in secondResponder

    PhysicalPlan plan = paginatedPlanCache.convertToPlan(firstResponder.getCursor().asString());
    var secondResponder = new TestResponder();
    paginatedQueryService.executePlan(plan, secondResponder);
  }

  private Settings defaultSettings() {
    return new Settings() {
      private final Map<Key, Integer> defaultSettings = new ImmutableMap.Builder<Key, Integer>()
          .put(Key.QUERY_SIZE_LIMIT, 200)
          .build();

      @Override
      public <T> T getSettingValue(Key key) {
        return (T) defaultSettings.get(key);
      }

      @Override
      public List<?> getSettings() {
        return (List<?>) defaultSettings;
      }
    };
  }
}
