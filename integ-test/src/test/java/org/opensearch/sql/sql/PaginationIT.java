/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class PaginationIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.CALCS);
    loadIndex(Index.BEER);
    loadIndex(Index.ONLINE);
  }

  @Test
  public void testSmallDataSet() throws IOException {
    var query = "SELECT * from " + TEST_INDEX_CALCS;
    var response = new JSONObject(executeFetchQuery(query, 4, "jdbc"));
    assertTrue(response.has("cursor"));
    assertEquals(4, response.getInt("total"));
    verifyIsV2Cursor(response);
  }

  @Test
  public void testLargeDataSet() throws IOException {
    var query = "SELECT * from " + TEST_INDEX_ONLINE;
    var response = new JSONObject(executeFetchQuery(query, 4, "jdbc"));
    assertTrue(response.has("cursor"));
    assertEquals(4, response.getInt("total"));
    verifyIsV2Cursor(response);

    var v1query = "SELECT * from " + TEST_INDEX_ONLINE + " WHERE 1 = 1";
    var v1response = new JSONObject(executeFetchQuery(v1query, 4, "jdbc"));
    assertTrue(v1response.has("cursor"));
    verifyIsV1Cursor(v1response);
  }

  private void verifyIsV2Cursor(JSONObject response) {
    if (!response.has("cursor")) {
      return;
    }

    var cursor = response.getString("cursor");
    if (cursor.isEmpty()) {
      return;
    }
    assertTrue("The cursor '" + cursor + "' is not from v2 engine.", cursor.startsWith("n:"));
  }

  private void verifyIsV1Cursor(JSONObject response) {
    if (!response.has("cursor")) {
      return;
    }

    var cursor = response.getString("cursor");
    if (cursor.isEmpty()) {
      return;
    }
    assertTrue("The cursor '" + cursor + "' is not from v2 engine.", cursor.startsWith("d:"));
  }

}
