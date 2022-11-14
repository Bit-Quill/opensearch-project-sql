/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WILDCARD;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class WildcardQueryIT extends PPLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.WILDCARD);
  }

  @Test
  public void test_wildcard_query_asterisk_function() throws IOException {
    String expected = "test wildcard";

    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, 't*') | head 1";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows(expected));
  }

  @Test
  public void test_wildcard_query_question_mark_function() throws IOException {
    String expected = "test wildcard";

    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, 'test wild??rd') | head 1";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows(expected));
  }

  //  SQL uses ? as a wildcard which is converted to * in WildcardQuery.java
  @Test
  public void test_wildcard_query_sql_wildcard_percent_conversion() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, 'test%') | head 1";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("test wildcard"));

    query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, 'test%')";
    result = executeQuery(query);
    assertEquals(8, result.getInt("total"));
  }

  //  SQL uses _ as a wildcard which is converted to ? in WildcardQuery.java
  @Test
  public void test_wildcard_query_sql_wildcard_underscore_conversion() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, 'test wild_ard') | head 1";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("test wildcard"));

    query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, 'test wild_ard*')";
    result = executeQuery(query);
    assertEquals(7, result.getInt("total"));
  }

  @Test
  public void test_escaping_wildcard_percent_in_the_beginning_of_text() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, '\\\\%*')";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("%test wildcard in the beginning of the text"));
  }

  @Test
  public void test_escaping_wildcard_percent_in_text() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, '*\\\\%%')";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("test wildcard in % the middle of the text"),
        rows("test wildcard %% beside each other"),
        rows("test wildcard in the end of the text%"),
        rows("%test wildcard in the beginning of the text"));
  }

  @Test
  public void test_escaping_wildcard_percent_in_the_end_of_text() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, '*\\\\%')";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("test wildcard in the end of the text%"));
  }

  @Test
  public void test_double_escaped_wildcard_percent() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, '*\\\\%\\\\%*')";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("test wildcard %% beside each other"));
  }

  @Test
  public void test_escaping_wildcard_underscore_in_the_beginning_of_text() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, '\\\\_*')";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("_test wildcard in the beginning of the text"));
  }

  @Test
  public void test_escaping_wildcard_underscore_in_text() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, '*\\\\_*')";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("test wildcard in _ the middle of the text"),
        rows("test wildcard __ beside each other"),
        rows("test wildcard in the end of the text_"),
        rows("_test wildcard in the beginning of the text"),
        rows("test backslash wildcard \\_"));
  }

  @Test
  public void test_escaping_wildcard_underscore_in_the_end_of_text() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, '*\\\\_')";
    JSONObject result = executeQuery(query);
    verifyDataRows(result,
        rows("test wildcard in the end of the text_"),
        rows("test backslash wildcard \\_"));
  }

  @Test
  public void test_double_escaped_wildcard_underscore() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, '*\\\\_\\\\_*')";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("test wildcard __ beside each other"));
  }

  @Test
  public void test_backslash_wildcard() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD + " | where wildcard_query(Body, '*\\\\\\\\\\\\_')";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("test backslash wildcard \\_"));
  }

  @Test
  public void all_params_test() throws IOException {
    String query = "source=" + TEST_INDEX_WILDCARD
        + " | wHERE wildcard_query(Body, 'test*', boost = 0.9,"
        + " case_insensitive=true, rewrite='constant_score')";
    JSONObject result = executeQuery(query);
    assertEquals(8, result.getInt("total"));
  }
}
