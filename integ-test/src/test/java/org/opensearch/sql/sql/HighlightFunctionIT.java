/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;
import java.util.List;

public class HighlightFunctionIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BEER);
  }

  @Test
  public void single_highlight_test() {
    String query = "SELECT Tags, highlight('Tags') FROM %s WHERE match(Tags, 'yeast') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("Tags", null, "text"),
        schema("highlight('Tags')", null, "nested"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void highlight_optional_arguments_test() {
    String query = "SELECT highlight('Tags', pre_tags='<mark>', post_tags='</mark>') " +
        "FROM %s WHERE match(Tags, 'yeast') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight('Tags', pre_tags='<mark>', post_tags='</mark>')",
            null, "nested"));

    assertEquals(1, response.getInt("total"));

    verifyDataRows(response,
        rows(new JSONArray(List.of("alcohol-level <mark>yeast</mark> home-brew champagne"))));
  }

  @Test
  public void highlight_multiple_optional_arguments_test() {
    String query = "SELECT highlight(Title), highlight(Body, pre_tags='<mark style=\\\"background-color: " +
        "green;\\\">', post_tags='</mark>') FROM %s WHERE multi_match([Title, Body], 'IPA') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));

    verifySchema(response, schema("highlight(Title)", null, "nested"),
        schema("highlight(Body, pre_tags='<mark style=\"background-color: green;\">', " +
                "post_tags='</mark>')", null, "nested"));

    assertEquals(1, response.getInt("total"));

    assertEquals("What are the differences between an <em>IPA</em> and its variants?",
        response.getJSONArray("datarows").getJSONArray(0).getJSONArray(0).getString(0));

    assertEquals("<p>I know what makes an <mark style=\"background-color: green;\">IPA</mark> an <mark style=" +
        "\"background-color: green;\">IPA</mark>, but what are the unique characteristics of it's common variants?",
        response.getJSONArray("datarows").getJSONArray(0).getJSONArray(1).getString(0));
  }

  @Test
  public void accepts_unquoted_test() {
    String query = "SELECT Tags, highlight(Tags) FROM %s WHERE match(Tags, 'yeast') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("Tags", null, "text"),
        schema("highlight(Tags)", null, "nested"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void multiple_highlight_test() {
    String query = "SELECT highlight(Title), highlight(Body) FROM %s WHERE MULTI_MATCH([Title, Body], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight(Title)", null, "nested"),
        schema("highlight(Body)", null, "nested"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void wildcard_highlight_test() {
    String query = "SELECT highlight('*itle') FROM %s WHERE MULTI_MATCH([Title, Body], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight('*itle')", null, "object"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void wildcard_multi_field_highlight_test() {
    String query = "SELECT highlight('T*') FROM %s WHERE MULTI_MATCH([Title, Tags], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight('T*')", null, "object"));
    var resultMap = response.getJSONArray("datarows").getJSONArray(0).getJSONObject(0);
    assertEquals(1, response.getInt("total"));
    assertTrue(resultMap.has("Title"));
    assertTrue(resultMap.has("Tags"));
  }

  @Test
  public void highlight_all_test() {
    String query = "SELECT highlight('*') FROM %s WHERE MULTI_MATCH([Title, Body], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight('*')", null, "object"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void highlight_no_limit_test() {
    String query = "SELECT highlight(Body) FROM %s WHERE MATCH(Body, 'hops')";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight(Body)", null, "nested"));
    assertEquals(2, response.getInt("total"));
  }
}
