/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_MULTI_NESTED;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

import java.io.IOException;
import java.util.List;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;

public class NestedIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.NESTED);
    loadIndex(Index.MULTI_NESTED);
  }

  @Test
  public void nested_function_with_array_of_nested_field_test() {
    String query = "SELECT nested(message.info), nested(comment.data) FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(5, result.getInt("total"));
    verifyDataRows(result,
        rows("a", "ab"),
        rows("b", "aa"),
        rows("c", "aa"),
        rows(new JSONArray(List.of("c","a")), "ab"),
        rows(new JSONArray(List.of("zz")), new JSONArray(List.of("aa", "bb"))));
  }

  @Test
  public void nested_function_with_array_of_multi_nested_field_test() {
    String query = "SELECT nested(message.author.name) FROM " + TEST_INDEX_MULTI_NESTED;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(5, result.getInt("total"));
    verifyDataRows(result,
        rows("e"),
        rows("f"),
        rows("g"),
        rows(new JSONArray(List.of("h", "p"))),
        rows(new JSONArray(List.of("yy"))));
  }
}
