/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

import java.io.IOException;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WILDCARD;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

public class LikeQueryIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.WILDCARD);
  }

  @Test
  public void test_like_in_select() throws IOException {
    String query = "SELECT Body, Body LIKE 'test wildcard%' FROM " + TEST_INDEX_WILDCARD;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("test wildcard", true),
        rows("test wildcard in the end of the text%", true),
        rows("%test wildcard in the beginning of the text", false),
        rows("test wildcard in % the middle of the text", true),
        rows("test wildcard %% beside each other", true),
        rows("test wildcard in the end of the text_", true),
        rows("_test wildcard in the beginning of the text", false),
        rows("test wildcard in _ the middle of the text", true),
        rows("test wildcard __ beside each other", true),
        rows("test backslash wildcard \\_", false));
  }

  @Test
  public void test_like_in_select_with_escaped_percent() throws IOException {
    String query = "SELECT Body, Body LIKE '\\\\%test wildcard%' FROM " + TEST_INDEX_WILDCARD;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("test wildcard", false),
        rows("test wildcard in the end of the text%", false),
        rows("%test wildcard in the beginning of the text", true),
        rows("test wildcard in % the middle of the text", false),
        rows("test wildcard %% beside each other", false),
        rows("test wildcard in the end of the text_", false),
        rows("_test wildcard in the beginning of the text", false),
        rows("test wildcard in _ the middle of the text", false),
        rows("test wildcard __ beside each other", false),
        rows("test backslash wildcard \\_", false));
  }

  @Test
  public void test_like_in_select_with_escaped_underscore() throws IOException {
    String query = "SELECT Body, Body LIKE '\\\\_test wildcard%' FROM " + TEST_INDEX_WILDCARD;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("test wildcard", false),
        rows("test wildcard in the end of the text%", false),
        rows("%test wildcard in the beginning of the text", false),
        rows("test wildcard in % the middle of the text", false),
        rows("test wildcard %% beside each other", false),
        rows("test wildcard in the end of the text_", false),
        rows("_test wildcard in the beginning of the text", true),
        rows("test wildcard in _ the middle of the text", false),
        rows("test wildcard __ beside each other", false),
        rows("test backslash wildcard \\_", false));
  }

  @Test
  public void test_like_in_where() throws IOException {
    String query = "SELECT Body FROM " + TEST_INDEX_WILDCARD + " WHERE Body LIKE 'test wildcard%'";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("test wildcard"),
        rows("test wildcard in the end of the text%"),
        rows("test wildcard in % the middle of the text"),
        rows("test wildcard %% beside each other"),
        rows("test wildcard in the end of the text_"),
        rows("test wildcard in _ the middle of the text"),
        rows("test wildcard __ beside each other"));
  }

  @Test
  public void test_like_in_where_with_escaped_percent() throws IOException {
    String query = "SELECT Body FROM " + TEST_INDEX_WILDCARD + " WHERE Body LIKE '\\\\%test wildcard%'";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("%test wildcard in the beginning of the text"));
  }

  @Test
  public void test_like_in_where_with_escaped_underscore() throws IOException {
    String query = "SELECT Body FROM " + TEST_INDEX_WILDCARD + " WHERE Body LIKE '\\\\_test wildcard%'";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("_test wildcard in the beginning of the text"));
  }
}
