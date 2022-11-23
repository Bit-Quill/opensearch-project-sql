  /*
   * Copyright OpenSearch Contributors
   * SPDX-License-Identifier: Apache-2.0
   */

package org.opensearch.sql.sql;

  import org.junit.Test;
  import org.opensearch.sql.legacy.SQLIntegTestCase;
  import java.io.IOException;


  import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TEXTKEYWORD;
  import static org.opensearch.sql.util.MatcherUtils.schema;
  import static org.opensearch.sql.util.MatcherUtils.verifySchema;

  public class TextTypeIT extends SQLIntegTestCase {


    @Override
    public void init() throws Exception {
      super.init();
      loadIndex(Index.TEXTKEYWORD);
    }

    @Test
    public void textKeywordTest() throws IOException {
      var result = executeJdbcRequest(String.format("select typeText from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("typeText", null, "text"));
    }

    @Test
    public void aggregateOnText() throws IOException {
      var result = executeJdbcRequest(String.format("select sum(int0) from %s GROUP BY typeText", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("sum(int0)", null, "integer"));
    }

    @Test
    public void aggregateOnKeyword() throws IOException {
      var result = executeJdbcRequest(String.format("select sum(int0) from %s GROUP BY typeKeyword", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("sum(int0)", null, "integer"));
    }

    @Test
    public void aggregateOnTextFieldData() throws IOException {
      var result = executeJdbcRequest(String.format("select sum(int0) from %s GROUP BY typeTextFieldData", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("sum(int0)", null, "integer"));
    }

    @Test
    public void aggregateOnKeywordFieldData() throws IOException {
      var result = executeJdbcRequest(String.format("select sum(int0) from %s GROUP BY typeKeywordFieldNoFieldData", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("sum(int0)", null, "integer"));
    }
  }
