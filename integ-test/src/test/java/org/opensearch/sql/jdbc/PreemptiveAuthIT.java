package org.opensearch.sql.jdbc;

import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

public class PreemptiveAuthIT extends SQLIntegTestCase {

  private static Connection connection;
  private boolean initialized = false;

  @BeforeEach
  @SneakyThrows
  public void init() {
    if (!initialized) {
      initClient();
      resetQuerySizeLimit();
      loadIndex(Index.BANK);
      loadIndex(Index.CALCS);
      loadIndex(Index.ONLINE);
      loadIndex(Index.ACCOUNT);
      initialized = true;
    }
  }

  @BeforeAll
  @SneakyThrows
  public static void initConnection() {
    var driverFile = ""; // TODO: add full path to local jdbc driver
    if (driverFile != null) {
      URLClassLoader loader = new URLClassLoader(
          new URL[]{new File(driverFile).toURI().toURL()},
          ClassLoader.getSystemClassLoader()
      );
      Driver driver = (Driver) Class.forName("org.opensearch.jdbc.Driver", true, loader)
          .getDeclaredConstructor().newInstance();
      java.util.Properties info = new java.util.Properties();
      info.put("user", "admin");
      info.put("password", "admin");
      info.put("usePreemptiveAuth", "true");
      connection = driver.connect(getConnectionString(), info);
    } else {
      connection = DriverManager.getConnection(getConnectionString());
    }
  }

  /**
   * Use OpenSearch cluster initialized by OpenSearch Gradle task.
   */
  private static String getConnectionString() {
    // string like "[::1]:46751,127.0.0.1:34403"
    var clusterUrls = System.getProperty("tests.rest.cluster").split(",");
    return String.format("jdbc:opensearch://%s", clusterUrls[clusterUrls.length - 1]);
  }

  @Test
  @SneakyThrows
  public void select_count_all_no_cursor() {
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_CALCS, TEST_INDEX_ONLINE, TEST_INDEX_BANK, TEST_INDEX_ACCOUNT)) {
      var query = String.format("SELECT COUNT(*) FROM %s", table);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
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
