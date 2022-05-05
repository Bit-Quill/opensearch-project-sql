/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PHRASE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class WhereCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.BANK);
    loadIndex(Index.GAME_OF_THRONES);
    loadIndex(Index.PHRASE);
  }

  @Test
  public void testWhereWithLogicalExpr() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname | where firstname='Amber' | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testWhereWithMultiLogicalExpr() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s "
                    + "| where firstname='Amber' lastname='Duke' age=32 "
                    + "| fields firstname, lastname, age",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber", "Duke", 32));
  }

  @Test
  public void testMultipleWhereCommands() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s "
                    + "| where firstname='Amber' "
                    + "| fields lastname, age"
                    + "| where lastname='Duke' "
                    + "| fields age "
                    + "| where age=32 "
                    + "| fields age",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows(32));
  }

  @Test
  public void testWhereEquivalentSortCommand() throws IOException {
    assertEquals(
        executeQueryToString(
            String.format("source=%s | where firstname='Amber'", TEST_INDEX_ACCOUNT)),
        executeQueryToString(String.format("source=%s firstname='Amber'", TEST_INDEX_ACCOUNT)));
  }

  @Test
  public void testLikeFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname | where like(firstname, 'Ambe_') | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testIsNullFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where isnull(age) | fields firstname",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(result, rows("Virginia"));
  }

  @Test
  public void testIsNotNullFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where isnotnull(age) and like(firstname, 'Ambe_') | fields firstname",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(result, rows("Amber JOHnny"));
  }

  @Test
  public void testRelevanceFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where match(firstname, 'Hattie') | fields firstname",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testMatchPhraseFunction() throws IOException {
    JSONObject result =
            executeQuery(
                    String.format(
                            "source=%s | where match_phrase(phrase, 'quick fox') | fields phrase", TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("quick fox"), rows("quick fox here"));
  }

  @Test
  public void testMathPhraseWithSlop() throws IOException {
    JSONObject result =
            executeQuery(
                    String.format(
                            "source=%s | where match_phrase(phrase, 'brown fox', slop = 2) | fields phrase", TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("brown fox"), rows("fox brown"));
  }

  @Test
  public void testMatchPhraseNoUnderscoreFunction() throws IOException {
    JSONObject result =
            executeQuery(
                    String.format(
                            "source=%s | where matchphrase(firstname, 'Hattie') | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testMathPhraseNoUnderscoreWithSlop() throws IOException {
    JSONObject result =
            executeQuery(
                    String.format(
                            "source=%s | where matchphrase(firstname, 'Hattie', slop = 2) | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"));
  }
}
