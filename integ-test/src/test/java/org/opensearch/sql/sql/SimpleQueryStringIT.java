/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PHRASE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class SimpleQueryStringIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.PHRASE);
  }

  @Test
  public void match_in_two_fields_test() throws IOException {
    String query = "SELECT * FROM "
        + TEST_INDEX_PHRASE + " WHERE simple_query_string(['*'], 'tea')";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    verifySchema(result,
        schema("phrase", "text"),
        schema("review", "text"),
        schema("test field", "long"),
        schema("insert_time2", "timestamp"));

    verifyDataRows(result,
        rows("black tea", "decent", null, null),
        rows("coffee and cream", "tea riffic", null, null));

  }

  @Test
  @EnabledIfEnvironmentVariable(named="WIP_TEST", matches = ".*")
  public void match_in_field_test() throws IOException {
    String query = "SELECT * FROM "
    + TEST_INDEX_PHRASE + " WHERE simple_query_string(['phrase'], 'tea')";
    var result = new JSONObject(executeQuery(query, "jdbc"));

    verifyDataRows(result,
        rows("black tea", "decent", null, null));
  }


  @Test
  public void no_matches_test() throws IOException {
    String query = "SELECT * FROM "
        + TEST_INDEX_PHRASE + " WHERE simple_query_string(['*'], 'rice')";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertEquals(0, result.getInt("total"));


  }
}
