/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class WildcardQueryIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BEER);
  }

  @Test
  public void test_wildcard_query_function() throws IOException {
    String query1 = "SELECT * FROM " + TEST_INDEX_BEER + " WHERE wildcard_query(Tags, 't*')";
    JSONObject result1 = executeJdbcRequest(query1);
    assertEquals(10, result1.getInt("total"));

    String query2 = "SELECT * FROM " + TEST_INDEX_BEER + " WHERE wildcardquery(Tags, 't*')";
    JSONObject result2 = executeJdbcRequest(query2);
    assertEquals(10, result2.getInt("total"));

    assertEquals(result1.getInt("total"), result2.getInt("total"));
  }

  @Test
  public void test_wildcard_query_sql_wildcard_conversion() throws IOException {
    // Test conversion from wildcard % to *
    String query1 = "SELECT * FROM " + TEST_INDEX_BEER + " WHERE wildcard_query(Tags, 't*')";
    JSONObject result1 = executeJdbcRequest(query1);
    assertEquals(10, result1.getInt("total"));

    String query2 = "SELECT * FROM " + TEST_INDEX_BEER + " WHERE wildcard_query(Tags, 't%')";
    JSONObject result2 = executeJdbcRequest(query2);
    assertEquals(10, result2.getInt("total"));

    assertEquals(result1.getInt("total"), result2.getInt("total"));


    // Test conversion from wildcard _ to ?
    String query3 = "SELECT * FROM " + TEST_INDEX_BEER + " WHERE wildcard_query(Tags, 'tast?')";
    JSONObject result3 = executeJdbcRequest(query3);
    assertEquals(8, result3.getInt("total"));

    String query4 = "SELECT * FROM " + TEST_INDEX_BEER + " WHERE wildcard_query(Tags, 'tast_')";
    JSONObject result4 = executeJdbcRequest(query4);
    assertEquals(8, result4.getInt("total"));

    assertEquals(result3.getInt("total"), result4.getInt("total"));
  }

  @Test
  public void all_params_test() throws IOException {
    String query = "SELECT Id FROM " + TEST_INDEX_BEER
        + "WHERE wildcard_query(Tags, 'tast_', boost = 0.9,"
        + "case_insensitive=true, rewrite='constant_score')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(8, result.getInt("total"));
  }
}
