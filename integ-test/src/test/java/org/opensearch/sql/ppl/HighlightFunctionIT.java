/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.io.IOException;

import com.google.errorprone.annotations.DoNotCall;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.TestsConstants;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

public class HighlightFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  @DoNotCall
  @Test
  public void test_single_highlight() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE match(Body, 'Cicerone') | highlight(Body)", TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));
    assertEquals(
        result.getJSONArray("datarows")
            .getJSONArray(0)
            .getString(18),
        equalTo("serving cicerone restaurants"));

    assertEquals(
        result.getJSONArray("schema")
            .getJSONObject(19)
            .getString("name"),
        equalTo("highlight(Title)"));
    assertEquals(
        result.getJSONArray("schema")
            .getJSONObject(19)
            .getString("type"),
        equalTo("string"));
  }

  @Test
  public void test_quoted_highlight() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
              "SOURCE=%s | WHERE match(Title, 'Cicerone') | highlight('Title')", TestsConstants.TEST_INDEX_BEER));
    assertEquals(1, result.getInt("total"));
  }

  @Test
  public void test_multiple_highlights() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE multi_match([Title, Body], 'hops') | highlight('Title') | highlight(Body)",
                TestsConstants.TEST_INDEX_BEER));
    assertEquals(2, result.getInt("total"));
  }
}
