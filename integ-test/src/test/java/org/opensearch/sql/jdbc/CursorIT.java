/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class CursorIT extends SQLIntegTestCase {

  private static Connection connection;

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.CALCS);
    loadIndex(Index.ONLINE);
    loadIndex(Index.ACCOUNT);
  }

  @AfterEach
  @After
  @SneakyThrows
  public void closeConnection() {
    // TODO should we close Statement and ResultSet?
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }

  @Test
  @SneakyThrows
  public void select_all_no_cursor() {
    connection = DriverManager.getConnection(getConnectionString());
    Statement stmt = connection.createStatement();

    // 22 vs 29 sec
    for (var table : List.of(TEST_INDEX_CALCS)){//, TEST_INDEX_ONLINE, TEST_INDEX_BANK, TEST_INDEX_ACCOUNT)) {
      var query = String.format("SELECT * FROM %s", table);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
    connection.close();
  }

  @Test
  @SneakyThrows
  public void select_count_all_no_cursor() {
    connection = DriverManager.getConnection(getConnectionString());
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_CALCS, TEST_INDEX_ONLINE, TEST_INDEX_BANK, TEST_INDEX_ACCOUNT)) {
      var query = String.format("SELECT COUNT(*) FROM %s", table);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
    connection.close();
  }

  @Test
  @SneakyThrows
  public void select_all_small_table_big_cursor() {
    connection = DriverManager.getConnection(getConnectionString());
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_CALCS, TEST_INDEX_BANK)) {
      var query = String.format("SELECT COUNT(*) FROM %s", table);
      stmt.setFetchSize(200);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
    connection.close();
  }

  @Test
  @SneakyThrows
  public void select_all_small_table_small_cursor() {
    connection = DriverManager.getConnection(getConnectionString());
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_CALCS, TEST_INDEX_BANK)) {
      var query = String.format("SELECT * FROM %s", table);
      stmt.setFetchSize(3);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
    connection.close();
  }

  @Test
  @SneakyThrows
  public void select_all_big_table_small_cursor() {
    connection = DriverManager.getConnection(getConnectionString());
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_ONLINE, TEST_INDEX_ACCOUNT)) {
      var query = String.format("SELECT * FROM %s", table);
      stmt.setFetchSize(10);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
    connection.close();
  }

  @Test
  @SneakyThrows
  public void select_all_big_table_big_cursor() {
    connection = DriverManager.getConnection(getConnectionString());
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_ONLINE, TEST_INDEX_ACCOUNT)) {
      var query = String.format("SELECT * FROM %s", table);
      stmt.setFetchSize(500);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
    connection.close();
  }

  /**
   * Use OpenSearch cluster initialized by OpenSearch Gradle task.
   */
  private String getConnectionString() {
    return String.format("jdbc:opensearch://%s", client().getNodes().get(0).getHost());
  }

  @SneakyThrows
  protected JSONObject executeRestQuery(String query, @Nullable Integer fetch_size) {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    if (fetch_size != null) {
      request.setJsonEntity(String.format("{ \"query\": \"%s\", \"fetch_size\": %d }", query, fetch_size));
    } else {
      request.setJsonEntity(String.format("{ \"query\": \"%s\" }", query));
    }

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
