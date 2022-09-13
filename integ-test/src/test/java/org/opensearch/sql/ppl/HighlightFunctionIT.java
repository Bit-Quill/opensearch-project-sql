/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.TestsConstants;

public class HighlightFunctionIT extends PPLIntegTestCase {
  // allFields is returned since we can't use highlight in a fields command.
  // Additional highlight fields begin at index 19
  private int firstHighlightFieldIndex = 19;
  private int secondHighlightFieldIndex = 20;
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  @Test
  public void single_highlight_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE match(Title, 'Cicerone') | highlight Title", TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));

    assertTrue(
        verifyFirstIndexHighlight(result,
            "What exactly is a <em>Cicerone</em>? What do they do?")
    );

    assertTrue(
        verifyFirstIndexHighlight(result, "What exactly is a <em>Cicerone</em>? What do they do?")
    );

    assertTrue(
        verifyHighlightSchema(result, "highlight Title")
    );
  }

  @Test
  public void highlight_optional_arguments_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE match(Title, 'Cicerone') | " +
                    "highlight Title, pre_tags='<mark>', post_tags='</mark>'", TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));

    assertTrue(
        verifyFirstIndexHighlight(result, "What exactly is a <mark>Cicerone</mark>? What do they do?")
    );

    assertTrue(
        verifyHighlightSchema(result, "highlight Title, pre_tags='<mark>', post_tags='</mark>'")
    );
  }

  @Test
  public void highlight_multiple_optional_arguments_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE multi_match([Title, Body], 'IPA') | highlight Title | highlight Body, " +
                    "pre_tags='<mark style=\\\"background-color: green;\\\">', post_tags='</mark>'",
                TestsConstants.TEST_INDEX_BEER));

    assertEquals(3, result.getInt("total"));

    assertTrue(
        verifyFirstIndexHighlight(result, "What are the differences between an <em>IPA</em> and its variants?")
    );

    assertTrue(
        verifySecondIndexHighlight(result,
            "<p>I know what makes an <mark style=\"background-color: green;\">IPA</mark> an " +
            "<mark style=\"background-color: green;\">IPA</mark>, but what are the unique characteristics " +
            "of it's common variants?")
    );
  }

  @Test
  public void quoted_highlight_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
              "SOURCE=%s | WHERE match(Title, 'Cicerone') | highlight 'Title'", TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));

    assertTrue(
        verifyFirstIndexHighlight(result,
            "What exactly is a <em>Cicerone</em>? What do they do?")
    );

  }

  @Test
  public void multiple_highlights_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE multi_match([Title, Body], 'IPA') | highlight 'Title' | highlight Body",
                TestsConstants.TEST_INDEX_BEER));

    assertEquals(3, result.getInt("total"));

    assertTrue(
        verifyFirstIndexHighlight(result,
            "What are the differences between an <em>IPA</em> and its variants?")
    );

    assertTrue(
        verifySecondIndexHighlight(result,
            "<p>I know what makes an <em>IPA</em> an <em>IPA</em>, but what are the unique " +
                "characteristics of it's common variants?")
    );
  }

  @Test
  public void highlight_wildcard_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE multi_match([Title, Body], 'Cicerone') | highlight 'T*'",
                TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));

    assertTrue(
        verifyFirstIndexHighlightWildcard(result, "Title",
            "What exactly is a <em>Cicerone</em>? What do they do?")
    );

    assertTrue(
        verifyHighlightSchema(result, "highlight 'T*'")
    );
  }

  @Test
  public void highlight_all_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE multi_match([Title, Body], 'Cicerone') | highlight '*'",
                TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));

    assertTrue(
        verifyFirstIndexHighlightWildcard(result, "Title",
            "What exactly is a <em>Cicerone</em>? What do they do?")
    );

    assertTrue(
        verifyFirstIndexHighlightWildcard(result, "Body",
            "<p>Recently I've started seeing references to the term '<em>Cicerone</em>' " +
                "pop up around the internet; generally")
    );
    assertTrue(
        verifyHighlightSchema(result, "highlight '*'")
    );
  }

  private boolean verifyFirstIndexHighlightWildcard(JSONObject result, String highlightField, String match) {
    return result.getJSONArray("datarows")
        .getJSONArray(0)
        .getJSONObject(firstHighlightFieldIndex)
        .getJSONArray(highlightField)
        .get(0)
        .equals(match);
  }

  private boolean verifySecondIndexHighlight(JSONObject result, String match) {
    return result.getJSONArray("datarows")
        .getJSONArray(0)
        .getJSONArray(secondHighlightFieldIndex)
        .getString(0)
        .equals(match);
  }

  private boolean verifyFirstIndexHighlight(JSONObject result, String match) {
    return result.getJSONArray("datarows")
        .getJSONArray(0)
        .getJSONArray(firstHighlightFieldIndex)
        .getString(0)
        .equals(match);
  }

  private boolean verifyHighlightSchema(JSONObject result, String name) {
    return result.getJSONArray("schema")
        .getJSONObject(firstHighlightFieldIndex)
        .getString("name")
        .equals(name);
  }
}
