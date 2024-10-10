/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;

public class AddFieldCommandIT extends PPLIntegTestCase {
  @Before
  public void beforeTest() throws IOException {
    setQuerySizeLimit(200);
  }

  @After
  public void afterTest() throws IOException {
    resetQuerySizeLimit();
    resetMaxResultWindow(TEST_INDEX_ACCOUNT);
  }

  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testAddField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname, age | head | addfield 'x' 'foo'",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(
        result,
        rows("Amber", 32, "foo"),
        rows("Hattie", 36, "foo"),
        rows("Nanette", 28, "foo"),
        rows("Dale", 33, "foo"),
        rows("Elinor", 36, "foo"),
        rows("Virginia", 39, "foo"),
        rows("Dillard", 34, "foo"),
        rows("Mcgee", 39, "foo"),
        rows("Aurelia", 37, "foo"),
        rows("Fulton", 23, "foo"));
  }
}
