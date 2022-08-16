/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

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
  public void test_match_phrase_function() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where match(Title, 'Cicerone') | highlight(Title)", TestsConstants.TEST_INDEX_BEER));
    assertEquals(1, result.getInt("total"));
    verifyDataRows(result, rows("What exactly is a Cicerone? What do they do?"));
  }
}