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

public class GeoPointIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.WILDCARD);
  }

  @Test
  public void test_geo_point_in_select() throws IOException {
    String query = "SELECT KeywordBody, KeywordBody LIKE 'test wildcard%' FROM " + TEST_INDEX_WILDCARD;
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
}
