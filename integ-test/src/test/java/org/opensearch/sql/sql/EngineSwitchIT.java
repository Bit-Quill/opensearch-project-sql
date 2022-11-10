/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class EngineSwitchIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.CALCS);
  }

  @Test
  @SneakyThrows
  public void test_no_param_set_v2_query() {
    var result = executeQueryOnEngine(String.format("SELECT COUNT(*) FROM %s", TEST_INDEX_CALCS), null);
    assertEquals(17, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
    assertTrue(findLogLineAfterAnotherLine(
        "Request is handled by new SQL query engine with fallback option to legacy", List.of(
            "Request is not supported and falling back to old SQL engine",
            "Request is handled by old SQL engine only",
            "Request is handled by new SQL query engine without fallback to legacy")));
  }

  @Test
  @SneakyThrows
  public void test_no_param_set_v1_query() {
    var result = executeQueryOnEngine(String.format("SELECT COUNT(*) FROM %s, %s",
        TEST_INDEX_CALCS, TEST_INDEX_CALCS), null);
    assertEquals(17, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
    assertTrue(findLogLineAfterAnotherLine(
        "Request is not supported and falling back to old SQL engine", List.of(
            "Request is handled by new SQL query engine with fallback option to legacy",
            "Request is handled by old SQL engine only",
            "Request is handled by new SQL query engine without fallback to legacy")));
  }

  // Any value, but 'v1`, 'legacy', 'v2' interpreted as no value set and should work with fallback
  @Test
  @SneakyThrows
  public void test_some_param_set_v2_query() {
    var result = executeQueryOnEngine(String.format("SELECT COUNT(*) FROM %s", TEST_INDEX_CALCS), "fallback");
    assertEquals(17, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
    assertTrue(findLogLineAfterAnotherLine(
        "Request is handled by new SQL query engine with fallback option to legacy", List.of(
            "Request is not supported and falling back to old SQL engine",
            "Request is handled by old SQL engine only",
            "Request is handled by new SQL query engine without fallback to legacy")));
  }

  @Test
  @SneakyThrows
  public void test_some_param_set_v1_query() {
    var result = executeQueryOnEngine(String.format("SELECT COUNT(*) FROM %s, %s",
        TEST_INDEX_CALCS, TEST_INDEX_CALCS), "null");
    assertEquals(17, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
    assertTrue(findLogLineAfterAnotherLine(
        "Request is not supported and falling back to old SQL engine", List.of(
            "Request is handled by new SQL query engine with fallback option to legacy",
            "Request is handled by old SQL engine only",
            "Request is handled by new SQL query engine without fallback to legacy")));
  }

  @Test
  @SneakyThrows
  public void test_v2_param_set_v2_query() {
    var result = executeQueryOnEngine("SELECT 1", "v2");
    assertEquals(1, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
    assertTrue(findLogLineAfterAnotherLine(
        "Request is handled by new SQL query engine without fallback to legacy", List.of(
            "Request is not supported and falling back to old SQL engine",
            "Request is handled by old SQL engine only",
            "Request is handled by new SQL query engine with fallback option to legacy")));
  }

  @Test
  @SneakyThrows
  public void test_v1_param_set_v2_query() {
    var exception = assertThrows(ResponseException.class, () -> executeQueryOnEngine("SELECT 1", "legacy"));
    assertTrue(exception.getMessage().contains("Invalid SQL query"));
    assertTrue(findLogLineAfterAnotherLine(
        "Request is handled by old SQL engine only", List.of(
            "Request is not supported and falling back to old SQL engine",
            "Request is handled by new SQL query engine without fallback to legacy",
            "Request is handled by new SQL query engine with fallback option to legacy")));
  }

  @Test
  @SneakyThrows
  public void test_v2_param_set_v1_query() {
    var exception = assertThrows(ResponseException.class, () ->
        executeQueryOnEngine(String.format("SELECT COUNT(*) FROM %s, %s",
            TEST_INDEX_CALCS, TEST_INDEX_CALCS), "v2"));
    assertTrue(exception.getMessage().contains("Invalid SQL query"));
    assertTrue(findLogLineAfterAnotherLine(
        "Request is handled by new SQL query engine without fallback to legacy", List.of(
            "Request is not supported and falling back to old SQL engine",
            "Request is handled by old SQL engine only",
            "Request is handled by new SQL query engine with fallback option to legacy")));
  }

  @Test
  @SneakyThrows
  public void test_v1_param_set_v1_query() {
    var result = executeQueryOnEngine(String.format("SELECT COUNT(*) FROM %s, %s",
        TEST_INDEX_CALCS, TEST_INDEX_CALCS), "v1");
    assertEquals(17, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
    assertTrue(findLogLineAfterAnotherLine(
        "Request is handled by old SQL engine only", List.of(
            "Request is not supported and falling back to old SQL engine",
            "Request is handled by new SQL query engine without fallback to legacy",
            "Request is handled by new SQL query engine with fallback option to legacy")));
  }

  /**
   * Function looks for the given line after another line(s) in IT cluster log from the end.
   * @return true if found.
   */
  @SneakyThrows
  private Boolean findLogLineAfterAnotherLine(String line, List<String> linesBefore) {
    var logDir = getAllClusterSettings().query("/defaults/path.logs");
    var lines = Files.readAllLines(Paths.get(logDir.toString(), "integTest.log"));
    for (String logLine : Lists.reverse(lines)) {
      if (logLine.contains(line)) {
        return true;
      }
      for (var lineBefore : linesBefore) {
        if (logLine.contains(lineBefore)) {
          return false;
        }
      }
    }
    return false;
  }

  protected JSONObject executeQueryOnEngine(String query, String engine) throws IOException {
    var endpoint = engine == null ? QUERY_API_ENDPOINT : QUERY_API_ENDPOINT + "?engine=" + engine;
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
