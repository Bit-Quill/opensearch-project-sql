/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.apache.commons.lang3.SystemUtils;
import org.json.JSONObject;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;

public class ShowCatalogsCommandIT extends PPLIntegTestCase {

  @BeforeClass
  public static void checkOs() {
    Assume.assumeTrue(SystemUtils.IS_OS_LINUX);
  }

  @Test
  public void testShowCatalogsCommands() throws IOException {
    JSONObject result = executeQuery("show catalogs");
    verifyDataRows(result,
        rows("my_prometheus", "PROMETHEUS"),
        rows("@opensearch", "OPENSEARCH"));
    verifyColumn(
        result,
        columnName("CATALOG_NAME"),
        columnName("CONNECTOR_TYPE")
    );
  }

  @Test
  public void testShowCatalogsCommandsWithWhereClause() throws IOException {
    JSONObject result = executeQuery("show catalogs | where CONNECTOR_TYPE='PROMETHEUS'");
    verifyDataRows(result,
        rows("my_prometheus", "PROMETHEUS"));
    verifyColumn(
        result,
        columnName("CATALOG_NAME"),
        columnName("CONNECTOR_TYPE")
    );
  }

}
