/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.startsWith;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PHRASE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.search.SearchHit;
import org.opensearch.sql.legacy.utils.StringUtils;

public class QueryFunctionsIT extends SQLIntegTestCase {

  private static final String FROM_ACCOUNTS = "FROM " + TEST_INDEX_ACCOUNT;
  private static final String FROM_NESTED = "FROM " + TEST_INDEX_NESTED_TYPE;
  private static final String FROM_PHRASE = "FROM " + TEST_INDEX_PHRASE;

  /**
   * TODO Looks like Math/Date Functions test all use the same query() and execute() functions
   * TODO execute/featureValueOf/hits functions are the same as used in NestedFieldQueryIT, should refactor into util
   */

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.NESTED);
    loadIndex(Index.PHRASE);
  }

  @Test
  public void query() {
    String query = "SELECT state " + FROM_ACCOUNTS + " WHERE QUERY('CA')";

    JSONObject result = executeJdbcRequest(query);
    assertEquals(17, result.getInt("total"));
    verifySchema(result, schema("state", "text"));
    verifyDataRows(result, rows("CA"),
            rows("CA"), rows("CA"),
            rows("CA"), rows("CA"),
            rows("CA"), rows("CA"),
            rows("CA"), rows("CA"),
            rows("CA"), rows("CA"),
            rows("CA"), rows("CA"),
            rows("CA"), rows("CA"),
            rows("CA"), rows("CA"));
  }

  @Test
  public void matchQueryRegularField() {
    String query = "SELECT firstname " + FROM_ACCOUNTS + " WHERE MATCH_QUERY(firstname, 'Ayers')";

    JSONObject result = executeJdbcRequest(query);
    assertEquals(1, result.getInt("total"));
    verifySchema(result, schema("firstname", "text"));
    verifyDataRows(result, rows("Ayers"));
  }

  @Test
  public void matchQueryNestedField() {
    String query = "SELECT comment.data " + FROM_NESTED
            + " WHERE MATCH_QUERY(NESTED(comment.data), 'aa')";

    JSONObject result = executeJdbcRequest(query);
    assertEquals(3, result.getInt("total"));
    verifySchema(result, schema("comment.data", "keyword"));
  }

  @Test
  public void scoreQuery() {
    String query = "SELECT firstname " + FROM_ACCOUNTS
            + " WHERE SCORE(MATCH_QUERY(firstname, 'Ayers'), 10)";

    JSONObject result = executeJdbcRequest(query);
    assertEquals(1, result.getInt("total"));
    verifySchema(result, schema("firstname", "text"));
    verifyDataRows(result, rows("Ayers"));
  }

  @Test
  public void scoreQueryWithNestedField() {
    String query = "SELECT comment.data " + FROM_NESTED
            + " WHERE SCORE(MATCH_QUERY(NESTED(comment.data), 'ab'), 10)";

    JSONObject result = executeJdbcRequest(query);
    assertEquals(2, result.getInt("total"));
    verifySchema(result, schema("comment.data", "keyword"));
    verifyDataRows(result, rows("ab"), rows("ab"));
  }

  @Test
  public void wildcardQuery() throws IOException {
    assertThat(
        query(
            "SELECT city",
            FROM_ACCOUNTS,
            "WHERE WILDCARD_QUERY(city.keyword, 'B*')"
        ),
        hits(
            hasFieldWithPrefix("city", "B")
        )
    );
  }

  @Test
  public void matchPhraseQuery() {
    String query = "SELECT phrase " + FROM_PHRASE + " WHERE MATCH_PHRASE(phrase, 'brown fox')";

    JSONObject result = executeJdbcRequest(query);
    assertEquals(1, result.getInt("total"));
    verifySchema(result, schema("phrase", "text"));
    verifyDataRows(result, rows("brown fox"));
  }

  @Test
  public void multiMatchQuerySingleField() {
    String query = "SELECT firstname " + FROM_ACCOUNTS
            + " WHERE MULTI_MATCH('query'='Ayers', 'fields'='firstname')";

    JSONObject result = executeJdbcRequest(query);
    assertEquals(1, result.getInt("total"));
    verifySchema(result, schema("firstname", "text"));
    verifyDataRows(result, rows("Ayers"));
  }

  @Test
  public void multiMatchQueryWildcardField() {
    String query = "SELECT firstname, lastname " + FROM_ACCOUNTS
            + " WHERE MULTI_MATCH('query'='Bradshaw', 'fields'='*name')";

    JSONObject result = executeJdbcRequest(query);
    assertEquals(2, result.getInt("total"));
    verifySchema(result, schema("firstname", "text"),
            schema("lastname", "text"));
    verifyDataRows(result, rows("Kayla", "Bradshaw"),
            rows("Bradshaw", "Mckenzie"));
  }

  @Test
  public void numberLiteralInSelectField() {
    assertTrue(
        executeQuery(StringUtils.format("SELECT 234234 AS number from %s", TEST_INDEX_ACCOUNT),
            "jdbc")
            .contains("234234")
    );

    assertTrue(
        executeQuery(StringUtils.format("SELECT 2.34234 AS number FROM %s", TEST_INDEX_ACCOUNT),
            "jdbc")
            .contains("2.34234")
    );
  }

  private final Matcher<SearchResponse> hits(Matcher<SearchHit> subMatcher) {
    return featureValueOf("hits", everyItem(subMatcher),
        resp -> Arrays.asList(resp.getHits().getHits()));
  }

  private <T, U> FeatureMatcher<T, U> featureValueOf(String name, Matcher<U> subMatcher,
                                                     Function<T, U> getter) {
    return new FeatureMatcher<T, U>(subMatcher, name, name) {
      @Override
      protected U featureValueOf(T actual) {
        return getter.apply(actual);
      }
    };
  }

  private final Matcher<SearchHit> hasFieldWithPrefix(String field, String prefix) {
    return featureValueOf(field, startsWith(prefix),
        hit -> (String) hit.getSourceAsMap().get(field));
  }

  private Matcher<SearchHit> kv(String key, Matcher<Object> valMatcher) {
    return featureValueOf(key, valMatcher, hit -> hit.getSourceAsMap().get(key));
  }

  /***********************************************************
   Query Utility to Fetch Response for SQL
   ***********************************************************/

  private SearchResponse query(String select, String from, String... statements)
      throws IOException {
    return execute(select + " " + from + " " + String.join(" ", statements));
  }

  private SearchResponse execute(String sql) throws IOException {
    final JSONObject jsonObject = executeQuery(sql);

    final XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
        NamedXContentRegistry.EMPTY,
        LoggingDeprecationHandler.INSTANCE,
        jsonObject.toString());
    return SearchResponse.fromXContent(parser);
  }
}
