  /*
   * Copyright OpenSearch Contributors
   * SPDX-License-Identifier: Apache-2.0
   */

package org.opensearch.sql.sql;

  import org.junit.Test;
  import org.opensearch.sql.legacy.SQLIntegTestCase;


  import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TEXTKEYWORD;
  import static org.opensearch.sql.util.MatcherUtils.schema;
  import static org.opensearch.sql.util.MatcherUtils.verifySchema;

  public class TextTypeIT extends SQLIntegTestCase {


    @Override
    public void init() throws Exception {
      super.init();
      loadIndex(Index.TEXTKEYWORD);
    }

    // Select

    @Test
    public void textKeywordTest() {
      var result = executeJdbcRequest(String.format("select typeKeyword from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("typeKeyword", null, "keyword"));
    }

    @Test
    public void aggregateOnText() {
      var result = executeJdbcRequest(String.format("select sum(int0) from %s GROUP BY typeText", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("sum(int0)", null, "integer"));
    }

    @Test
    public void aggregateOnKeyword() {
      var result = executeJdbcRequest(String.format("select sum(int0) from %s GROUP BY typeKeyword", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("sum(int0)", null, "integer"));
    }

    @Test
    public void aggregateOnTextFieldData() {
      var result = executeJdbcRequest(String.format("select sum(int0) from %s GROUP BY typeTextFieldData", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("sum(int0)", null, "integer"));
    }

    @Test
    public void aggregateOnKeywordFieldData() {
      var result = executeJdbcRequest(String.format("select sum(int0) from %s GROUP BY typeKeywordFieldNoFieldData", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("sum(int0)", null, "integer"));
    }

    @Test
    public void aggregateOnTextAndFieldDataNoFields() {
      var result = executeJdbcRequest(String.format("select sum(int0) from %s GROUP BY textDataFieldNoFields", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("sum(int0)", null, "integer"));
    }

    // Where like

    @Test
    public void whereLikeKeyword() {
      executeJdbcRequest(String.format("select * from %s WHERE typeKeyword LIKE \\\"key*\\\"", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void whereLikeText() {
      executeJdbcRequest(String.format("select * from %s WHERE typeText LIKE \\\"text*\\\"", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void whereLikeKeywordFieldNoFieldData() {
      executeJdbcRequest(String.format("select * from %s WHERE typeKeywordFieldNoFieldData LIKE \\\"keyword*\\\"", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void whereLikeTextFieldData() {
      executeJdbcRequest(String.format("select * from %s WHERE typeTextFieldData LIKE \\\"keyFD*\\\"", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void whereLiketextDataFieldNoFields() {
      executeJdbcRequest(String.format("select * from %s WHERE textDataFieldNoFields LIKE \\\"textFDNF*\\\"", TEST_INDEX_TEXTKEYWORD));
    }

    // Wildcard

    @Test
    public void whereWildcardKeyword() {
      executeJdbcRequest(String.format("select * from %s WHERE wildcard_query(typeKeyword,  \\\"key*\\\")", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void whereWildcardText() {
      executeJdbcRequest(String.format("select * from %s WHERE wildcard_query(\\\"typeText\\\",  \\\"text*\\\")", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void whereWildcardKeywordFieldNoFieldData() {
      executeJdbcRequest(String.format("select * from %s WHERE wildcard_query(\\\"typeKeywordFieldNoFieldData\\\",  \\\"keyword*\\\")", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void whereWildcardTextFieldData() {
      executeJdbcRequest(String.format("select * from %s WHERE wildcard_query(\\\"typeTextFieldData\\\",  \\\"keyFD*\\\")", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void whereWildcardtextDataFieldNoFields() {
      executeJdbcRequest(String.format("select * from %s WHERE wildcard_query(\\\"textDataFieldNoFields\\\",  \\\"textFDNF*\\\")", TEST_INDEX_TEXTKEYWORD));
    }

    // Locate

    @Test
    public void selectLocateKeyword() {
      executeJdbcRequest(String.format("select typeKeyword LIKE \\\"key*\\\" from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectLocateText() {
      executeJdbcRequest(String.format("select typeText LIKE \\\"text*\\\" from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectLocateTextKeywordFieldNoFieldData() {
      executeJdbcRequest(String.format("select typeKeywordFieldNoFieldData LIKE \\\"keyword*\\\" from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectLocateTypeTextFieldData() {
      executeJdbcRequest(String.format("select typeTextFieldData LIKE \\\"keyFD*\\\" from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectLocateTextDataFieldNoFields() {
      executeJdbcRequest(String.format("select textDataFieldNoFields LIKE \\\"textFDNF*\\\" from %s", TEST_INDEX_TEXTKEYWORD));
    }

    // Position

    @Test
    public void selectPositionKeyword() {
      executeJdbcRequest(String.format("select POSITION(key IN typeKeyword) from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectPositionText() {
      executeJdbcRequest(String.format("select POSITION(text IN typeText) from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectPositionTextKeywordFieldNoFieldData() {
      executeJdbcRequest(String.format("select POSITION(keyword IN typeKeywordFieldNoFieldData) from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectPositionTypeTextFieldData() {
      executeJdbcRequest(String.format("select POSITION(keyFD IN typeTextFieldData) from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectPositionTextDataFieldNoFields() {
      executeJdbcRequest(String.format("select POSITION(textFDNF IN textDataFieldNoFields) from %s", TEST_INDEX_TEXTKEYWORD));
    }

    // Substring

    @Test
    public void selectSubstringKeyword() {
      executeJdbcRequest(String.format("select SUBSTRING(typeKeyword, 1, 1) from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectSubstringText() {
      executeJdbcRequest(String.format("select SUBSTRING(typeText, 1, 1) from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectSubstringTextKeywordFieldNoFieldData() {
      executeJdbcRequest(String.format("select SUBSTRING(typeKeywordFieldNoFieldData, 1, 1) from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectSubstringTypeTextFieldData() {
      executeJdbcRequest(String.format("select SUBSTRING(typeTextFieldData, 1, 1) from %s", TEST_INDEX_TEXTKEYWORD));
    }

    @Test
    public void selectSubstringTextDataFieldNoFields() {
      executeJdbcRequest(String.format("select  SUBSTRING(textDataFieldNoFields, 1, 1) from %s", TEST_INDEX_TEXTKEYWORD));
    }
  }
