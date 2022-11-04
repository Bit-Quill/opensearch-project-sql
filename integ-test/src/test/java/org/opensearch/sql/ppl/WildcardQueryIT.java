/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class WildcardQueryIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  @Test
  public void wildcard_query_function_test() throws IOException {
    String query = "source=" + TEST_INDEX_BEER + " | where wildcard_query(Tags, 't*')";
    JSONObject result = executeQuery(query);
    assertEquals(10, result.getInt("total"));
  }

  @Test
  public void test_wildcard_query_sql_wildcard_conversion() throws IOException {
    // Test conversion from wildcard % to *
    String query1 = "source=" + TEST_INDEX_BEER + " | where wildcard_query(Tags, 't*')";
    JSONObject result1 = executeQuery(query1);
    assertEquals(10, result1.getInt("total"));

    String query2 = "source=" + TEST_INDEX_BEER + " | where wildcard_query(Tags, 't%')";
    JSONObject result2 = executeQuery(query2);
    assertEquals(10, result2.getInt("total"));

    assertEquals(result1.getInt("total"), result2.getInt("total"));


    // Test conversion from wildcard _ to ?
    String query3 = "source=" + TEST_INDEX_BEER + " | where wildcard_query(Tags, 'tast?')";
    JSONObject result3 = executeQuery(query3);
    assertEquals(8, result3.getInt("total"));

    String query4 = "source=" + TEST_INDEX_BEER + " | where wildcard_query(Tags, 'tast_')";
    JSONObject result4 = executeQuery(query4);
    assertEquals(8, result4.getInt("total"));

    assertEquals(result3.getInt("total"), result4.getInt("total"));
  }

  @Test
  public void all_params_test() throws IOException {
    String query = "source=" + TEST_INDEX_BEER
        + "| where wildcard_query(Tags, 'tast_', boost = 0.9,"
        + "case_insensitive=true, rewrite='constant_score')";
    JSONObject result = executeQuery(query);
    assertEquals(8, result.getInt("total"));
  }
}
