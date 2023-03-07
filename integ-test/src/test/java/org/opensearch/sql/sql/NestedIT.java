/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_MULTI_NESTED_TYPE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class NestedIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.MULTI_NESTED);
    loadIndex(Index.NESTED);
    loadIndex(Index.NESTED_WITHOUT_ARRAYS);
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
  public void nested_function_in_select_test() {
    String query = "SELECT nested(message.info), nested(comment.data), "
        + "nested(message.dayOfWeek) FROM "
        + TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(5, result.getInt("total"));
    verifySchema(result,
        schema("nested(message.info)", null, "keyword"),
        schema("nested(comment.data)", null, "keyword"),
        schema("nested(message.dayOfWeek)", null, "long"));
    verifyDataRows(result,
        rows("a", "ab", 1),
        rows("b", "aa", 2),
        rows("c", "aa", 1),
        rows("c", "ab", 4),
        rows("zz", "bb", 6));
  }

  // Has to be tested with JSON format when https://github.com/opensearch-project/sql/issues/1317
  // gets resolved
  @Test
  public void nested_function_in_an_aggregate_function_in_select_test() {
    String query = "SELECT sum(nested(message.dayOfWeek)) FROM " +
        TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows(14));
  }

  @Test
  public void nested_function_with_arrays_in_an_aggregate_function_in_select_test() {
    String query = "SELECT sum(nested(message.dayOfWeek)) FROM " +
        TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows(19));
  }

  @Test
  public void nested_function_in_a_function_in_select_test() {
    String query = "SELECT upper(nested(message.info)) FROM " +
        TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
    JSONObject result = executeJdbcRequest(query);

    verifyDataRows(result,
        rows("A"),
        rows("B"),
        rows("C"),
        rows("C"),
        rows("ZZ"));
  }


  @Test
  public void nested_function_with_array_of_multi_nested_field_test() {
    String query = "SELECT nested(message.author.name) FROM " + TEST_INDEX_MULTI_NESTED_TYPE;
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