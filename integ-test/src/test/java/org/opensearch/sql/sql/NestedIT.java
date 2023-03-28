/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_EMPLOYEE_NESTED;
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
import org.junit.jupiter.api.Disabled;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class NestedIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.MULTI_NESTED);
    loadIndex(Index.NESTED);
    loadIndex(Index.NESTED_WITHOUT_ARRAYS);
    loadIndex(Index.EMPLOYEE_NESTED);
  }

  @Test
  public void nested_function_with_array_of_nested_field_test() {
    String query = "SELECT nested(message.info), nested(comment.data) FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(6, result.getInt("total"));
    verifyDataRows(result,
        rows("c", "ab"),
        rows("a", "ab"),
        rows("b", "aa"),
        rows("c", "aa"),
        rows("a", "ab"),
        rows("zz", new JSONArray(List.of("aa", "bb"))));
  }

  @Test
  public void nested_function_in_select_test() {
    String query = "SELECT nested(message.info), nested(comment.data), "
        + "nested(message.dayOfWeek) FROM "
        + TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(5, result.getInt("total"));
    verifySchema(result,
        schema("message.info", null, "keyword"),
        schema("comment.data", null, "keyword"),
        schema("message.dayOfWeek", null, "long"));
    verifyDataRows(result,
        rows("a", "ab", 1),
        rows("b", "aa", 2),
        rows("c", "aa", 1),
        rows("c", "ab", 4),
        rows("zz", "bb", 6));
  }

  // Has to be tested with JSON format when https://github.com/opensearch-project/sql/issues/1317
  // gets resolved
  @Disabled // TODO fix me when aggregation is supported
  public void nested_function_in_an_aggregate_function_in_select_test() {
    String query = "SELECT sum(nested(message.dayOfWeek)) FROM " +
        TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows(14));
  }

  // TODO Enable me when nested aggregation is supported
  @Disabled
  public void nested_function_with_arrays_in_an_aggregate_function_in_select_test() {
    String query = "SELECT sum(nested(message.dayOfWeek)) FROM " +
        TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows(19));
  }

  // TODO not currently supported by legacy, should we add implementation in AstBuilder?
  @Disabled
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

    assertEquals(6, result.getInt("total"));
    verifyDataRows(result,
        rows("e"),
        rows("f"),
        rows("g"),
        rows("h"),
        rows("p"),
        rows("yy"));
  }

  @Test
  public void nested_with_non_nested_type_test() {
    String query = "SELECT nested(someField) FROM " + TEST_INDEX_NESTED_TYPE;

    Exception exception = assertThrows(RuntimeException.class,
        () -> executeJdbcRequest(query));
    assertTrue(exception.getMessage().contains(
            "{\n" +
            "  \"error\": {\n" +
            "    \"reason\": \"Invalid SQL query\",\n" +
            "    \"details\": \"Illegal nested field name: someField\",\n" +
            "    \"type\": \"IllegalArgumentException\"\n" +
            "  },\n" +
            "  \"status\": 400\n" +
            "}"
    ));
  }

  @Test
  public void nested_missing_path() {
    String query = "SELECT nested(message.invalid) FROM " + TEST_INDEX_MULTI_NESTED_TYPE;

    Exception exception = assertThrows(RuntimeException.class,
        () -> executeJdbcRequest(query));
    assertTrue(exception.getMessage().contains("" +
        "{\n" +
        "  \"error\": {\n" +
        "    \"reason\": \"Invalid SQL query\",\n" +
        "    \"details\": \"can't resolve Symbol(namespace=FIELD_NAME, name=message.invalid) in type env\",\n" +
        "    \"type\": \"SemanticCheckException\"\n" +
        "  },\n" +
        "  \"status\": 400\n" +
        "}"
    ));
  }

  @Test
  public void nested_missing_path_argument() {
    String query = "SELECT nested(message.author.name, invalid) FROM " + TEST_INDEX_MULTI_NESTED_TYPE;

    Exception exception = assertThrows(RuntimeException.class,
        () -> executeJdbcRequest(query));
    assertTrue(exception.getMessage().contains("" +
        "{\n" +
        "  \"error\": {\n" +
        "    \"reason\": \"Invalid SQL query\",\n" +
        "    \"details\": \"can't resolve Symbol(namespace=FIELD_NAME, name=invalid) in type env\",\n" +
        "    \"type\": \"SemanticCheckException\"\n" +
        "  },\n" +
        "  \"status\": 400\n" +
        "}"
    ));
  }
}
