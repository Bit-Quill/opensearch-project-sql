/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class NestedIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.NESTED);
  }

  @Test
  public void nested_string_subfield_test() {
    String query = "SELECT nested(message.info) FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);
    assertEquals(5, result.getInt("total"));
  }

  @Test
  public void test_nested_where_with_and_conditional() {
    String query = "SELECT nested(message.info), nested(message.author) FROM " + TEST_INDEX_NESTED_TYPE
        + " WHERE nested(message, message.info = 'a' AND message.author = 'e')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1, result.getInt("total"));
    verifyDataRows(result, rows("a", "e"));
  }

  @Test
  public void test_nested_where_as_predicate_expression() {
    String query = "SELECT nested(message.info) FROM " + TEST_INDEX_NESTED_TYPE
        + " WHERE nested(message.info) = 'a'";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(2, result.getInt("total"));
    // Returns whole array with each containing 'message.info'. Maybe not how we want to handle in future.
    verifyDataRows(result, rows("a"), rows(new JSONArray(List.of("c", "a"))));
  }
}
