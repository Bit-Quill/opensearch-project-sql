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
      loadIndex(Index.CALCS);

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
      var result = executeJdbcRequest(String.format("select typeKeyword from %s WHERE typeKeyword LIKE \\\"key*\\\"", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("typeKeyword", null, "keyword"));
    }

    @Test
    public void whereLikeText() {
      var result = executeJdbcRequest(String.format("select typeText from %s WHERE typeText LIKE \\\"text*\\\"", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("typeText", null, "text"));
    }

    @Test
    public void whereLikeKeywordFieldNoFieldData() {
      var result = executeJdbcRequest(String.format("select typeKeywordFieldNoFieldData from %s WHERE typeKeywordFieldNoFieldData LIKE \\\"keyword*\\\"", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("typeKeywordFieldNoFieldData", null, "text"));
    }

    @Test
    public void whereLikeTextFieldData() {
      var result = executeJdbcRequest(String.format("select typeTextFieldData from %s WHERE typeTextFieldData LIKE \\\"keyFD*\\\"", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("typeTextFieldData", null, "text"));
    }

    @Test
    public void whereLiketextDataFieldNoFields() {
      var result = executeJdbcRequest(String.format("select textDataFieldNoFields from %s WHERE textDataFieldNoFields LIKE \\\"textFDNF*\\\"", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("textDataFieldNoFields", null, "text"));
    }

    // Wildcard

    @Test
    public void whereWildcardKeyword() {
      var result = executeJdbcRequest(String.format("select typeKeyword from %s WHERE wildcard_query(typeKeyword,  \\\"key*\\\")", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("typeKeyword", null, "keyword"));
    }

    @Test
    public void whereWildcardText() {
      var result = executeJdbcRequest(String.format("select typeText from %s WHERE wildcard_query(\\\"typeText\\\",  \\\"text*\\\")", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("typeText", null, "text"));
    }

    @Test
    public void whereWildcardKeywordFieldNoFieldData() {
      var result = executeJdbcRequest(String.format("select typeKeywordFieldNoFieldData from %s WHERE wildcard_query(\\\"typeKeywordFieldNoFieldData\\\",  \\\"keyword*\\\")", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("typeKeywordFieldNoFieldData", null, "text"));
    }

    @Test
    public void whereWildcardTextFieldData() {
      var result = executeJdbcRequest(String.format("select typeTextFieldData from %s WHERE wildcard_query(\\\"typeTextFieldData\\\",  \\\"keyFD*\\\")", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("typeTextFieldData", null, "text"));
    }

    @Test
    public void whereWildcardtextDataFieldNoFields() {
      var result = executeJdbcRequest(String.format("select textDataFieldNoFields from %s WHERE wildcard_query(\\\"textDataFieldNoFields\\\",  \\\"textFDNF*\\\")", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("textDataFieldNoFields", null, "text"));
    }

    // Locate

    @Test
    public void selectLocateKeyword() {
      var result = executeJdbcRequest(String.format("select locate(\\\"key*\\\", typeKeyword) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("locate(\"key*\", typeKeyword)", null, "integer"));
    }

    @Test
    public void selectLocateText() {
      var result = executeJdbcRequest(String.format("select locate(\\\"text*\\\", typeText) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("locate(\"text*\", typeText)", null, "integer"));
    }

    @Test
    public void selectLocateTextKeywordFieldNoFieldData() {
      var result = executeJdbcRequest(String.format("select locate(\\\"keyword*\\\", typeKeywordFieldNoFieldData) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("locate(\"keyword*\", typeKeywordFieldNoFieldData)", null, "integer"));
    }

    @Test
    public void selectLocateTypeTextFieldData() {
      var result = executeJdbcRequest(String.format("select locate(\\\"keyFD*\\\", typeTextFieldData) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("locate(\"keyFD*\", typeTextFieldData)", null, "integer"));
    }

    @Test
    public void selectLocateTextDataFieldNoFields() {
      var result = executeJdbcRequest(String.format("select locate(\\\"textFDNF*\\\", textDataFieldNoFields) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("locate(\"textFDNF*\", textDataFieldNoFields)", null, "integer"));
    }

    // Position

    @Test
    public void selectPositionKeyword() {
      var result = executeJdbcRequest(String.format("select POSITION(\\\"key\\\" IN typeKeyword) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("LOCATE('key', typeKeyword)", null, "double"));
    }

    @Test
    public void selectPositionText() throws IOException {
      var result = executeQuery(String.format("select POSITION(\\\"text\\\" IN typeText) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("typeText", null, "double"));

//      }
    }

    @Test
    public void selectPositionTextKeywordFieldNoFieldData() {
      var result = executeJdbcRequest(String.format("select POSITION(\\\"keyword\\\" IN typeKeywordFieldNoFieldData) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("LOCATE('keyword', typeKeywordFieldNoFieldData)", null, "double"));
    }

    @Test
    public void selectPositionTypeTextFieldData() throws IOException {
      var result = executeQuery(String.format("select POSITION(\\\"keyFD\\\" IN typeTextFieldData) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("LOCATE('keyFD', typeTextFieldData)", null, "double"));
    }

    @Test
    public void selectPositionTextDataFieldNoFields() {
      var result = executeJdbcRequest(String.format("select POSITION(\\\"textFDNF\\\" IN textDataFieldNoFields) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("LOCATE('textFDNF', textDataFieldNoFields)", null, "double"));
    }

    // Substring

    @Test
    public void selectSubstringKeyword() {
      var result = executeJdbcRequest(String.format("select SUBSTRING(typeKeyword, 1, 1) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("SUBSTRING(typeKeyword, 1, 1)", null, "keyword"));
    }

    @Test
    public void selectSubstringText() {
      var result = executeJdbcRequest(String.format("select SUBSTRING(typeText, 1, 1) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("SUBSTRING(typeText, 1, 1)", null, "keyword"));
    }

    @Test
    public void selectSubstringTextKeywordFieldNoFieldData() {
      var result = executeJdbcRequest(String.format("select SUBSTRING(typeKeywordFieldNoFieldData, 1, 1) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("SUBSTRING(typeKeywordFieldNoFieldData, 1, 1)", null, "keyword"));
    }

    @Test
    public void selectSubstringTypeTextFieldData() {
      var result = executeJdbcRequest(String.format("select SUBSTRING(typeTextFieldData, 1, 1) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("SUBSTRING(typeTextFieldData, 1, 1)", null, "keyword"));
    }

    @Test
    public void selectSubstringTextDataFieldNoFields() {
      var result = executeJdbcRequest(String.format("select  SUBSTRING(textDataFieldNoFields, 1, 1) from %s", TEST_INDEX_TEXTKEYWORD));
      verifySchema(result,
          schema("SUBSTRING(textDataFieldNoFields, 1, 1)", null, "keyword"));
    }
  }
