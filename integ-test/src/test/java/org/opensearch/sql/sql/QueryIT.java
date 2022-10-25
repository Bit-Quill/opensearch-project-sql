/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class QueryIT extends SQLIntegTestCase {
    @Override
    public void init() throws IOException {
        loadIndex(Index.BEER);
    }

    @Test
    public void all_fields_test() throws IOException {
        String query = "SELECT * FROM "
                + TEST_INDEX_BEER + " WHERE query('*:taste')";
        JSONObject result = executeJdbcRequest(query);
        assertEquals(16, result.getInt("total"));
    }

    @Test
    public void mandatory_params_test() throws IOException {
        String query = "SELECT Id FROM "
                + TEST_INDEX_BEER + " WHERE query('Tags:taste OR Body:taste')";
        JSONObject result = executeJdbcRequest(query);
        assertEquals(16, result.getInt("total"));
    }
}
