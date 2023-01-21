/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

import java.io.IOException;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;

public class NestedIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.NESTED);
  }

  // Incorrect expected result
//  @Test
//  public void nested_string_subfield_test() {
//    String query = "SELECT nested(message.dayOfWeek) FROM " + TEST_INDEX_NESTED_TYPE;
//    JSONObject result = executeJdbcRequest(query);
//    assertEquals(5, result.getInt("total"));
//  }

  @Test
  public void nested_function_with_array_of_nested_field_test() {
    String query = "SELECT nested(message.info) FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("a"),
        rows("b"),
        rows("c"),
        rows("c"),
        rows("a"),
        rows("zz"));
  }
}
