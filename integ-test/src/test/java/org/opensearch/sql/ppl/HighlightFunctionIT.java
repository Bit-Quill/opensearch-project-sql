/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.TestsConstants;

public class HighlightFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  @Test
  public void test_single_highlight() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE match(Title, 'Cicerone') | highlight(Title)", TestsConstants.TEST_INDEX_BEER));
    assertEquals(1, result.getInt("total"));
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