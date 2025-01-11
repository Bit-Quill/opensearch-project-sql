/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_JSON_TEST;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class JsonFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.JSON_TEST);
  }

  @Test
  public void test_json_valid() throws IOException {
    JSONObject result;

    result =
        executeQuery(
            String.format(
                "source=%s | where json_valid(json_string) | fields test_name",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"));
    verifyDataRows(
        result,
        rows("json object"),
        rows("json array"),
        rows("json scalar string"),
        rows("json empty string"));
  }

  @Test
  public void test_not_json_valid() throws IOException {
    JSONObject result;

    result =
        executeQuery(
            String.format(
                "source=%s | where not json_valid(json_string) | fields test_name",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"));
    verifyDataRows(result, rows("json invalid object"));
  }

  @Test
  public void test_json_object() throws IOException {
    JSONObject result;

    result =
        executeQuery(
            String.format(
                "source=%s | eval obj=json_object(\"key\", json(json_string)) | fields test_name, obj"
                    + " test_name, casted",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"), schema("casted", null, "undefined"));
    verifyDataRows(
        result,
        rows("json object", Map.of("key", Map.of("a", "1", "b", "2"))),
        rows("json array", Map.of("key", List.of(1, 2, 3, 4))),
        rows("json scalar string", Map.of("key", "abc")),
        rows("json empty string", Map.of("key", null))
    );
  }
}
