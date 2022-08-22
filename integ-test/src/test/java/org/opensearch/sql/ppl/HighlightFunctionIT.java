/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

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

    assertTrue(
        result.getJSONArray("datarows")
            .getJSONArray(0)
            .getJSONArray(19)
            .getString(0)
            .equals("What exactly is a <em>Cicerone</em>? What do they do?"));

    assertTrue(
        result.getJSONArray("schema")
            .getJSONObject(19)
            .getString("name")
            .equals("highlight(Title)"));

    assertTrue(
        result.getJSONArray("schema")
            .getJSONObject(19)
            .getString("type")
            .equals("string"));
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
