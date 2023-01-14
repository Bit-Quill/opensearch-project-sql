/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import java.io.IOException;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.rows;

public class NestedFallbackIT extends SQLIntegTestCase{
  @Override
  public void init() throws IOException {
    loadIndex(Index.NESTED);
  }

  @Test
  public void nested_function_and_without_nested_function_test() {
    String query = "SELECT nested(message.info), message.info FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);

    verifyColumn(result, schema("message.info", "keyword"),
        schema("message.info", "keyword"));
    verifyDataRows(result, rows("a", "a"),
        rows("b", "b"),
        rows("c", "c"),
        rows("c", "c"),
        rows("a", "a"),
        rows("zz", "zz"));
  }

  @Ignore("Legacy has a bug that returns nulls for array values. " +
      "This will be fixed in the new engine. https://github.com/opensearch-project/sql/issues/1218")
  @Test
  public void without_nested_function_test() {
    String query = "SELECT message.info FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);

    verifyColumn(result, schema("message.info", "keyword"));
    verifyDataRows(result, rows("a"),
        rows("b"),
        rows("c"),
        rows("c"),
        rows("a"),
        rows("zz"));
  }
}
