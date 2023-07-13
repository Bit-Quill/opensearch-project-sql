/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ARRAYS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class ArraysIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.ARRAYS);
  }

  @Test
  public void object_array_index_1_test() {
    String query = "SELECT objectArray[0] FROM " + TEST_INDEX_ARRAYS;
    JSONObject result = executeJdbcRequest(query);

    verifyDataRows(result,
        rows(new JSONObject(ImmutableMap.of("innerObject", List.of(1, 2)))));
  }

  @Test
  public void object_array_index_1_inner_object_test() {
    String query = "SELECT objectArray[0].innerObject FROM " + TEST_INDEX_ARRAYS;
    JSONObject result = executeJdbcRequest(query);

    verifyDataRows(result,
        rows(new JSONArray(List.of(1, 2))));
  }

  @Test
  public void object_array_index_1_inner_object_index_1_test() {
    String query = "SELECT objectArray[0].innerObject[0] FROM " + TEST_INDEX_ARRAYS;
    JSONObject result = executeJdbcRequest(query);

    verifyDataRows(result,
        rows(1));
  }

  @Test
  public void multi_object_array_index_1_test() {
    String query = "SELECT multiObjectArray[0] FROM " + TEST_INDEX_ARRAYS;
    JSONObject result = executeJdbcRequest(query);

    verifyDataRows(result,
        rows(new JSONObject(ImmutableMap.of("id", 1, "name", "blah"))));
  }
}
