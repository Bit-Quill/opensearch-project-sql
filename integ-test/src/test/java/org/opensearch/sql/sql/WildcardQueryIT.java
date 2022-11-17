/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WILDCARD;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

public class WildcardQueryIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.WILDCARD);
  }

  @Test
  public void test_wildcard_query_asterisk_function() throws IOException {
    String expected = "test wildcard";

    String query1 = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, 't*') LIMIT 1";
    JSONObject result1 = executeJdbcRequest(query1);
    verifyDataRows(result1, rows(expected));

    String query2 = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcardquery(Body, 't*') LIMIT 1";
    JSONObject result2 = executeJdbcRequest(query2);
    verifyDataRows(result2, rows(expected));
  }

  @Test
  public void test_LIKE() throws IOException {
    String expected = "test wildcard";

    String query1 = "SELECT Body, Body LIKE 't*' FROM " + TEST_INDEX_WILDCARD + " LIMIT 1";
    JSONObject result1 = executeJdbcRequest(query1);
    verifyDataRows(result1, rows(expected));

    query1 = "SELECT Body FROM " + TEST_INDEX_WILDCARD + " WHERE Body LIKE 't*' LIMIT 1";
    result1 = executeJdbcRequest(query1);
    verifyDataRows(result1, rows(expected));
  }

  @Test
  public void test_wildcard_query_question_mark_function() throws IOException {
    String expected = "test wildcard";

    String query1 = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, 'test wild??rd')";
    JSONObject result1 = executeJdbcRequest(query1);
    verifyDataRows(result1, rows(expected));

    String query2 = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcardquery(Body, 'test wild??rd')";
    JSONObject result2 = executeJdbcRequest(query2);
    verifyDataRows(result2, rows(expected));
  }

  //  SQL uses ? as a wildcard which is converted to * in WildcardQuery.java
  @Test
  public void test_wildcard_query_sql_wildcard_percent_conversion() throws IOException {
    String query1 = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, 'test%') LIMIT 1";
    JSONObject result1 = executeJdbcRequest(query1);
    verifyDataRows(result1, rows("test wildcard"));

    query1 = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, 'test%')";
    result1 = executeJdbcRequest(query1);
    assertEquals(8, result1.getInt("total"));

    String query2 = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, 'test*')";
    JSONObject result2 = executeJdbcRequest(query2);
    assertEquals(result1.getInt("total"), result2.getInt("total"));
  }

  //  SQL uses _ as a wildcard which is converted to ? in WildcardQuery.java
  @Test
  public void test_wildcard_query_sql_wildcard_underscore_conversion() throws IOException {
    String query1 = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, 'test wild_ard') LIMIT 1";
    JSONObject result1 = executeJdbcRequest(query1);
    verifyDataRows(result1, rows("test wildcard"));

    query1 = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, 'test wild_ard*')";
    result1 = executeJdbcRequest(query1);
    assertEquals(7, result1.getInt("total"));

    String query2 = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, 'test wild?ard*')";
    JSONObject result2 = executeJdbcRequest(query2);
    assertEquals(result1.getInt("total"), result2.getInt("total"));
  }

  @Test
  public void test_escaping_wildcard_percent_in_the_beginning_of_text() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, '\\\\%*')";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows("%test wildcard in the beginning of the text"));
  }

  @Test
  public void test_escaping_wildcard_percent_in_text() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, '*\\\\%%')";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows("test wildcard in % the middle of the text"),
        rows("test wildcard %% beside each other"),
        rows("test wildcard in the end of the text%"),
        rows("%test wildcard in the beginning of the text"));
  }

  @Test
  public void test_escaping_wildcard_percent_in_the_end_of_text() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, '*\\\\%')";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows("test wildcard in the end of the text%"));
  }

  @Test
  public void test_double_escaped_wildcard_percent() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, '*\\\\%\\\\%*')";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows("test wildcard %% beside each other"));
  }

  @Test
  public void test_escaping_wildcard_underscore_in_the_beginning_of_text() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, '\\\\_*')";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows("_test wildcard in the beginning of the text"));
  }

  @Test
  public void test_escaping_wildcard_underscore_in_text() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, '*\\\\_*')";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows("test wildcard in _ the middle of the text"),
        rows("test wildcard __ beside each other"),
        rows("test wildcard in the end of the text_"),
        rows("_test wildcard in the beginning of the text"),
        rows("test backslash wildcard \\_"));
  }

  @Test
  public void test_escaping_wildcard_underscore_in_the_end_of_text() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, '*\\\\_')";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("test wildcard in the end of the text_"),
        rows("test backslash wildcard \\_"));
  }

  @Test
  public void test_double_escaped_wildcard_underscore() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, '*\\\\_\\\\_*')";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows("test wildcard __ beside each other"));
  }

  @Test
  public void test_backslash_wildcard() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE wildcard_query(Body, '*\\\\\\\\\\\\_')";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows("test backslash wildcard \\_"));
  }

  @Test
  public void all_params_test() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD
        + " WHERE wildcard_query(Body, 'test*', boost = 0.9,"
        + " case_insensitive=true, rewrite='constant_score')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(8, result.getInt("total"));
  }
}
