/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

import java.io.IOException;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;

public class NestedIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.NESTED);
  }

  @Test
  public void nested_string_subfield_test() {
    String query = "SELECT nested(message.dayOfWeek) FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);
    assertEquals(5, result.getInt("total"));
  }

  @Test
  public void nested_column_name_test() {
    String fieldArg = "message.info";
    String query = "SELECT nested(" + fieldArg + ") FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);
    verifyColumn(result, schema(fieldArg, "keyword"));
  }

  @Test
  public void nested_alias_test() {
    String fieldArg = "message.info";
    String alias = "INFO";
    String query = "SELECT nested(" + fieldArg + ") AS " + alias + " FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);
    verifyColumn(result, schema(fieldArg, alias, "keyword"));
  }
}
