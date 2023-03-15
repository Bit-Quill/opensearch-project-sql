/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.datasource.model.DataSourceMetadata.defaultOpenSearchDataSourceMetadata;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.SneakyThrows;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.DataSourceServiceImpl;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.PaginatedPlanCache;
import org.opensearch.sql.executor.execution.PaginatedQueryService;
import org.opensearch.sql.expression.DSL;
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

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class StandalonePaginationIT extends SQLIntegTestCase {

  private PaginatedQueryService paginatedQueryService;

  private PaginatedPlanCache paginatedPlanCache;

  private OpenSearchClient client;

  @Override
  @SneakyThrows
  public void init() {
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.ONLINE);
    loadIndex(Index.BEER);
    loadIndex(Index.BANK);
    if (!isIndexExist(client(), "empty")) {
      executeRequest(new Request("PUT", "/empty"));
    }

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
  public void test_pagination_whitebox() throws IOException {
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
    LogicalPlan p = new LogicalPaginate(1, List.of(
        new LogicalProject(
          new LogicalRelation("test", t), List.of(
                DSL.named("name", DSL.ref("name", ExprCoreType.STRING)),
                DSL.named("age", DSL.ref("age", ExprCoreType.LONG))),
                List.of()
    )));
    var firstResponder = new TestResponder();
    paginatedQueryService.executePlan(p, firstResponder);

    // act 2, asserts in secondResponder

    PhysicalPlan plan = paginatedPlanCache.convertToPlan(firstResponder.getCursor().toString());
    var secondResponder = new TestResponder();
    paginatedQueryService.executePlan(plan, secondResponder);

    // act 3: confirm that there's no cursor.
  }

  // Test takes 3+ min due to a big amount of requests issued
  // Skip 'online' index and/or page_size = 1 to get a significant speed-up
  @Test
  @SneakyThrows
  public void test_pagination_blackbox() {
    var indices = getResponseBody(client().performRequest(new Request("GET", "_cat/indices?h=i")), true).split("\n");
    for (var index : indices) {
      var response = executeJdbcRequest(String.format("select * from %s", index));
      var indexSize = response.getInt("total");
      var rows = response.getJSONArray("datarows");
      var schema = response.getJSONArray("schema");
      for (var pageSize : List.of(1, 5, 10, 100, 1000)) {
        var testReportPrefix = String.format("index: %s, page size: %d || ", index, pageSize);
        var rowsPaged = new JSONArray();
        var rowsReturned = 0;
        response = new JSONObject(executeFetchQuery(
            String.format("select * from %s", index), pageSize, "jdbc"));
        while (response.has("cursor")) {
          var cursor = response.getString("cursor");
          assertTrue(testReportPrefix + "Cursor returned from legacy engine",
              cursor.startsWith("n:"));
          rowsReturned += response.getInt("size");
          var datarows = response.getJSONArray("datarows");
          for (int i = 0; i < datarows.length(); i++) {
            rowsPaged.put(datarows.get(i));
          }
          assertTrue("Paged response schema doesn't match to non-paged",
              schema.similar(response.getJSONArray("schema")));
          assertEquals(indexSize, response.getInt("total"));
          response = executeCursorQuery(cursor);
        }
        assertEquals(testReportPrefix + "Last page is not empty",
            0, response.getInt("total"));
        assertEquals(testReportPrefix + "Last page is not empty",
            0, response.getJSONArray("datarows").length());
        assertEquals(testReportPrefix + "Paged responses return another row count that non-paged",
            indexSize, rowsReturned);
        assertTrue(testReportPrefix + "Paged accumulated result has other rows than non-paged",
            rows.similar(rowsPaged));
      }
    }
  }

  @Test
  @SneakyThrows
  public void test_explain_not_supported() {
    var request = new Request("POST", "_plugins/_sql/_explain");
    // Request should be rejected before index names are resolved
    request.setJsonEntity("{ \"query\": \"select * from something\", \"fetch_size\": 10 }");
    var exception = assertThrows(ResponseException.class, () -> client().performRequest(request));
    var response = new JSONObject(new String(exception.getResponse().getEntity().getContent().readAllBytes()));
    assertEquals("`explain` feature for paginated requests is not implemented yet.",
        response.getJSONObject("error").getString("details"));

    // Request should be rejected before cursor parsed
    request.setJsonEntity("{ \"cursor\" : \"n:0000\" }");
    exception = assertThrows(ResponseException.class, () -> client().performRequest(request));
    response = new JSONObject(new String(exception.getResponse().getEntity().getContent().readAllBytes()));
    assertEquals("Explain of a paged query continuation is not supported. Use `explain` for the initial query request.",
        response.getJSONObject("error").getString("details"));
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
