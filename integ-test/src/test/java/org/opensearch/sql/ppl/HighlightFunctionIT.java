/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.io.IOException;
import java.util.List;
import com.google.common.collect.ImmutableMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.legacy.TestsConstants;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

public class HighlightFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  @Test
  public void single_highlight_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE match(Title, 'Cicerone') | fields highlight(Title)", TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));

    verifySchema(result, schema("highlight(Title)", null, "array"));
    verifyDataRows(result, rows(new JSONArray(List.of("What exactly is a <em>Cicerone</em>? What do they do?"))));

  }

  @Test
  public void highlight_optional_arguments_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE match(Title, 'Cicerone') | fields " +
                    "highlight(Title, pre_tags='<mark>', post_tags='</mark>')", TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));

    verifyDataRows(result, rows(new JSONArray(List.of("What exactly is a <mark>Cicerone</mark>? What do they do?"))));
    verifySchema(result, schema("highlight(Title, pre_tags='<mark>', post_tags='</mark>')",
        null, "array"));
  }

  @Test
  public void highlight_multiple_optional_arguments_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE multi_match([Title, Body], 'IPA') | fields highlight(Title), highlight(Body, " +
                    "pre_tags='<mark style=\\\"background-color: green;\\\">', post_tags='</mark>') | head 1",
                TestsConstants.TEST_INDEX_BEER));

    verifyDataRows(result, rows(new JSONArray(List.of("What are the differences between an <em>IPA</em> and its variants?")),
        new JSONArray(List.of("<p>I know what makes an <mark style=\"background-color: green;\">IPA</mark> an <mark style=\"background-color: green;\">IPA</mark>, but what are the unique characteristics of it's common variants?",
            "To be specific, the ones I'm interested in are Double <mark style=\"background-color: green;\">IPA</mark> and Black <mark style=\"background-color: green;\">IPA</mark>, but general differences between"))));
  }

  @Test
  public void quoted_highlight_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
              "SOURCE=%s | WHERE match(Title, 'Cicerone') | fields highlight('Title')", TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));
    verifyDataRows(result, rows(new JSONArray(List.of("What exactly is a <em>Cicerone</em>? What do they do?"))));
  }

  @Test
  public void multiple_highlights_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE multi_match([Title, Body], 'IPA') | fields highlight('Title'), highlight(Body) | HEAD 1",
                TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));
    verifyDataRows(result, rows(new JSONArray(List.of("What are the differences between an <em>IPA</em> and its variants?")),
        new JSONArray(List.of("<p>I know what makes an <em>IPA</em> an <em>IPA</em>, but what are the unique characteristics of it's common variants?",
            "To be specific, the ones I'm interested in are Double <em>IPA</em> and Black <em>IPA</em>, but general differences between"))));
  }

  @Test
  public void highlight_wildcard_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE multi_match([Title, Body], 'Cicerone') | fields highlight('T*')",
                TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));
    verifyDataRows(result, rows(new JSONObject(ImmutableMap.of(
        "Title", new JSONArray(List.of("What exactly is a <em>Cicerone</em>? What do they do?"))))));
    verifySchema(result, schema("highlight('T*')", null, "struct"));
  }

  @Test
  public void highlight_all_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SOURCE=%s | WHERE multi_match([Title, Body], 'Cicerone') | fields highlight('*')",
                TestsConstants.TEST_INDEX_BEER));

    assertEquals(1, result.getInt("total"));
    verifySchema(result, schema("highlight('*')", null, "struct"));

    verifyDataRows(result, rows(new JSONObject(ImmutableMap.of(
        "Title", new JSONArray(List.of("What exactly is a <em>Cicerone</em>? What do they do?")),
        "Body", new JSONArray(List.of("<p>Recently I've started seeing references to the term '<em>Cicerone</em>' " +
            "pop up around the internet; generally", "What exactly does a <em>cicerone</em> <strong>do</strong>?"))
    ))));
  }

  @Test
  public void highlight_semantic_check_test() throws SemanticCheckException {
    String query = String.format("SOURCE=%s | WHERE multi_match([Title, Body], 'Cicerone') | fields - highlight('*')",
        TestsConstants.TEST_INDEX_BEER);
    queryShouldThrowSemanticException(query, "can't resolve Symbol highlight('*') in type env");
  }

  private void queryShouldThrowSemanticException(String query, String... messages) {
    try {
      executeQuery(query);
      fail("Expected to throw SemanticCheckException, but none was thrown for query: " + query);
    } catch (ResponseException e) {
      String errorMsg = e.getMessage();
      assertTrue(errorMsg.contains("SemanticCheckException"));
      for (String msg : messages) {
        assertTrue(errorMsg.contains(msg));
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected exception raised for query: " + query);
    }
  }
}
