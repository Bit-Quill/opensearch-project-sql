/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.rest.RestStatus;
import org.opensearch.sql.legacy.utils.StringUtils;

public class QueryIT extends SQLIntegTestCase {

  /**
   * Currently commenting out tests related to JoinType index since there is an issue with mapping.
   * <p>
   * Also ignoring the following tests as they are failing, will require investigation:
   * - idsQuerySubQueryIds
   * - escapedCharactersCheck
   * - fieldCollapsingTest
   * - idsQueryOneId
   * - idsQueryMultipleId
   * - multipleIndicesOneNotExistWithoutHint
   * <p>
   * The following tests are being ignored because subquery is still running in OpenSearch transport thread:
   * - twoSubQueriesTest()
   * - inTermsSubQueryTest()
   */

  final static int BANK_INDEX_MALE_TRUE = 4;
  final static int BANK_INDEX_MALE_FALSE = 3;

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ONLINE);
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.PHRASE);
    loadIndex(Index.DOG);
    loadIndex(Index.PEOPLE);
    loadIndex(Index.GAME_OF_THRONES);
    loadIndex(Index.ODBC);
    loadIndex(Index.LOCATION);
    loadIndex(Index.NESTED);
    // TODO Remove comment after issue with loading join type is resolved
    // loadIndex(Index.JOIN);
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_TWO);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void queryEndWithSemiColonTest() {
    executeJdbcRequest(StringUtils.format("SELECT * FROM %s;", TEST_INDEX_BANK));
  }

  @Test
  public void searchTypeTest() {
    JSONObject response = executeJdbcRequest(String.format( "SELECT * FROM %s LIMIT 1000",
        TestsConstants.TEST_INDEX_PHRASE));
    assertEquals(6, response.getInt("total"));
  }

  @Test
  public void multipleFromTest() {
    JSONObject response = executeJdbcRequest(String.format(Locale.ROOT,
        "SELECT * FROM %s, %s LIMIT 2000",
        TestsConstants.TEST_INDEX_BANK, TestsConstants.TEST_INDEX_BANK_TWO));
    assertEquals(14, response.getInt("total"));
  }

  @Test
  public void selectAllWithFieldReturnsAll() throws IOException {
    // When run using executeJdbcRequest(),
    // fails due to 'java.lang.IllegalArgumentException: Multiple entries with same key' error'
    JSONObject response = executeQuery(StringUtils.format(
        "SELECT *, age " +
            "FROM %s " +
            "LIMIT 5",
        TestsConstants.TEST_INDEX_BANK
    ));

    checkSelectAllAndFieldResponseSize(response);
  }

  @Test
  public void selectAllWithFieldReverseOrder() throws IOException {
    // When run using executeJdbcRequest(),
    // fails due to 'java.lang.IllegalArgumentException: Multiple entries with same key' error'
    JSONObject response = executeQuery(StringUtils.format(
        "SELECT *, age " +
            "FROM %s " +
            "LIMIT 5",
        TestsConstants.TEST_INDEX_BANK
    ));

    checkSelectAllAndFieldResponseSize(response);
  }

  @Test
  public void selectAllWithMultipleFields() throws IOException {
    // When run using executeJdbcRequest(),
    // fails due to 'java.lang.IllegalArgumentException: Multiple entries with same key' error'
    JSONObject response = executeQuery(StringUtils.format(
        "SELECT *, age, address " +
            "FROM %s " +
            "LIMIT 5",
        TestsConstants.TEST_INDEX_BANK
    ));

    checkSelectAllAndFieldResponseSize(response);
  }

  @Test
  public void selectAllWithFieldAndOrderBy() throws IOException {
    // When run using executeJdbcRequest(),
    // fails due to 'java.lang.IllegalArgumentException: Multiple entries with same key' error'
    JSONObject response = executeQuery(StringUtils.format(
        "SELECT *, age " +
            "FROM %s " +
            "ORDER BY age " +
            "LIMIT 5",
        TestsConstants.TEST_INDEX_BANK
    ));

    checkSelectAllAndFieldResponseSize(response);
  }

  @Test
  public void selectAllWithFieldAndGroupBy() throws IOException {
    // When run using executeJdbcRequest(),
    // fails due to 'java.lang.IllegalArgumentException: Multiple entries with same key' error'
    JSONObject response = executeQuery(StringUtils.format(
        "SELECT *, age " +
            "FROM %s " +
            "GROUP BY age " +
            "LIMIT 10",
        TestsConstants.TEST_INDEX_BANK
    ));

    checkSelectAllAndFieldAggregationResponseSize(response, "age");
  }

  @Test
  public void selectAllWithFieldAndGroupByReverseOrder() throws IOException {
    // When run using executeJdbcRequest(),
    // fails due to 'java.lang.IllegalArgumentException: Multiple entries with same key' error'
    JSONObject response = executeQuery(StringUtils.format(
        "SELECT *, age " +
            "FROM %s " +
            "GROUP BY age " +
            "LIMIT 10",
        TestsConstants.TEST_INDEX_BANK
    ));

    checkSelectAllAndFieldAggregationResponseSize(response, "age");
  }

  @Test
  public void selectFieldWithAliasAndGroupBy() {
    JSONObject response =
        executeJdbcRequest("SELECT lastname AS name FROM " + TEST_INDEX_ACCOUNT + " GROUP BY name");
    JSONObject schema = response.getJSONArray("schema").getJSONObject(0);
    assertTrue(schema.has("name"));
    assertTrue(schema.has("alias"));
  }

  public void indexWithWildcardTest() {
    JSONObject response = executeJdbcRequest(String.format("SELECT * FROM %s* LIMIT 1000",
        TestsConstants.TEST_INDEX_BANK));
    assertTrue(response.getInt("total") > 0);
  }

  @Test
  public void selectSpecificFields() {
    String[] arr = new String[] {"age", "account_number"};
    Set<String> expectedSource = new HashSet<>(Arrays.asList(arr));

    JSONObject response =
        executeJdbcRequest(String.format("SELECT age, account_number FROM %s",
            TEST_INDEX_ACCOUNT));
    assertResponseForSelectSpecificFields(response, expectedSource);
  }

  @Test
  public void selectSpecificFieldsUsingTableAlias() {
    String[] arr = new String[] {"age", "account_number"};
    Set<String> expectedSource = new HashSet<>(Arrays.asList(arr));

    JSONObject response =
        executeJdbcRequest(String.format("SELECT a.age, a.account_number FROM %s a",
            TEST_INDEX_ACCOUNT));
    assertResponseForSelectSpecificFields(response, expectedSource);
  }

  @Test
  public void selectSpecificFieldsUsingTableNamePrefix() {
    String[] arr = new String[] {"age", "account_number"};
    Set<String> expectedSource = new HashSet<>(Arrays.asList(arr));

    JSONObject response = executeJdbcRequest(String.format(
        "SELECT opensearch-sql_test_index_account.age, opensearch-sql_test_index_account.account_number"
                + " FROM %s",
        TEST_INDEX_ACCOUNT));
    assertResponseForSelectSpecificFields(response, expectedSource);
  }

  private void assertResponseForSelectSpecificFields(JSONObject response,
                                                     Set<String> expectedSource) {
    Set<String> actualSource = new HashSet<>();

    assertTrue(response.getInt("total") > 0);
    JSONArray schema = response.getJSONArray("schema");
    for (int i = 0; i < schema.length(); ++i) {
      String name = (String) schema.getJSONObject(i).get("name");
      actualSource.add(name);
    }
    assertEquals(expectedSource, actualSource);
  }

  @Test
  public void selectFieldWithSpace() {
    String[] arr = new String[] {"test field"};
    Set<String> expectedSource = new HashSet<>(Arrays.asList(arr));

    JSONObject response = executeJdbcRequest(String.format(Locale.ROOT, "SELECT ['test field'] FROM %s " +
            "WHERE ['test field'] IS NOT null",
        TestsConstants.TEST_INDEX_PHRASE));

    assertResponseForSelectSpecificFields(response, expectedSource);
  }

  @Ignore("field aliases are not supported currently")
  // it might be possible to change field names after the query already executed.
  @Test
  public void selectAliases() {

    String[] arr = new String[] {"myage", "myaccount_number"};
    Set<String> expectedSource = new HashSet<>(Arrays.asList(arr));

    JSONObject result = executeJdbcRequest(String.format(
        "SELECT age AS myage, account_number AS myaccount_number FROM %s", TEST_INDEX_ACCOUNT));
    assertResponseForSelectSpecificFields(result, expectedSource);
  }

  @Test
  public void useTableAliasInWhereClauseTest() {
    JSONObject response = executeJdbcRequest(String.format(
        "SELECT * FROM %s a WHERE a.city = 'Nogal' LIMIT 1000", TEST_INDEX_ACCOUNT));

    assertEquals(response.getInt("total"), 1);
    verifyDataRows(response, rows(13,"Nanette","789 Madison Street",32838,"F",
            "Nogal","Quility","VA",28,"nanettebates@quility.com","Bates"));
  }

  @Test
  public void notUseTableAliasInWhereClauseTest() {
    JSONObject response = executeJdbcRequest(String.format(
        "SELECT * FROM %s a WHERE city = 'Nogal' LIMIT 1000", TEST_INDEX_ACCOUNT));

    assertEquals(response.getInt("total"), 1);
    verifyDataRows(response, rows(13,"Nanette","789 Madison Street",32838,"F",
            "Nogal","Quility","VA",28,"nanettebates@quility.com","Bates"));
  }

  @Test
  public void useTableNamePrefixInWhereClauseTest() {
    JSONObject response = executeJdbcRequest(String.format(
        "SELECT * FROM %s WHERE opensearch-sql_test_index_account.city = 'Nogal' LIMIT 1000",
        TEST_INDEX_ACCOUNT
    ));

    assertEquals(response.getInt("total"), 1);
    verifyDataRows(response, rows(13,"Nanette","789 Madison Street",32838,"F",
            "Nogal","Quility","VA",28,"nanettebates@quility.com","Bates"));
  }

  @Test
  public void equalityTest() {
    JSONObject response = executeJdbcRequest(String.format(Locale.ROOT,
        "SELECT * FROM %s WHERE city = 'Nogal' LIMIT 1000", TEST_INDEX_ACCOUNT));

    assertEquals(response.getInt("total"), 1);
    verifyDataRows(response, rows(13,"Nanette","789 Madison Street",32838,"F",
            "Nogal","Quility","VA",28,"nanettebates@quility.com","Bates"));
  }

  @Test
  public void equalityTestPhrase() {
    JSONObject response = executeJdbcRequest(String.format("SELECT * FROM %s WHERE " +
            "match_phrase(phrase, 'quick fox here') LIMIT 1000",
        TestsConstants.TEST_INDEX_PHRASE));

    assertEquals(response.getInt("total"), 1);
    verifyDataRows(response,
            rows("quick fox here",null,"2014-08-19 07:09:13.434"));
  }

  @Test
  public void greaterThanTest() {
    int someAge = 25;
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE age > %s LIMIT 1000",
            TestsConstants.TEST_INDEX_PEOPLE,
            someAge));

    assertEquals(response.getInt("total"), 11);
  }

  @Test
  public void greaterThanOrEqualTest() {
    int someAge = 25;
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE age >= %s LIMIT 1000",
            TEST_INDEX_ACCOUNT,
            someAge));

    assertEquals(response.getInt("total"), 775);

    boolean isEqualFound = false;
    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);
      int age = hit.getInt(getFieldIndex(response, "age"));
      assertTrue(age >= someAge);

        if (age == someAge) {
            isEqualFound = true;
        }
    }

    assertTrue(
        String.format("At least one of the documents need to contains age equal to %s",
            someAge),
        isEqualFound);
  }

  @Test
  public void lessThanTest() {
    int someAge = 25;
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE age < %s LIMIT 1000",
            TestsConstants.TEST_INDEX_PEOPLE,
            someAge));

    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);
      int age = hit.getInt(getFieldIndex(response, "age"));
      assertTrue(age < someAge);
    }
  }

  @Test
  public void lessThanOrEqualTest() {
    int someAge = 25;
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE age <= %s LIMIT 1000",
            TEST_INDEX_ACCOUNT,
            someAge));

    boolean isEqualFound = false;
    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);
      int age = hit.getInt(getFieldIndex(response, "age"));
      assertTrue(age <= someAge);

        if (age == someAge) {
            isEqualFound = true;
        }
    }

    Assert.assertTrue(
        String.format("At least one of the documents need to contains age equal to %s",
            someAge),
        isEqualFound);
  }

  @Test
  public void orTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * " +
            "FROM %s " +
            "WHERE match_phrase(gender, 'F') OR match_phrase(gender, 'M') " +
            "LIMIT 1000", TEST_INDEX_ACCOUNT));
    assertEquals(1000, response.getInt("total"));
  }

  @Test
  public void andTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE age=32 AND gender='M' LIMIT 1000",
            TestsConstants.TEST_INDEX_PEOPLE));

    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);
      assertEquals(32, hit.getInt(getFieldIndex(response, "age")));
      assertEquals("M", hit.getString(getFieldIndex(response, "gender")));
    }
  }

  @Test
  public void likeTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE firstname LIKE 'amb%%' LIMIT 1000",
            TEST_INDEX_ACCOUNT));

    JSONArray datarows = response.getJSONArray("datarows");
    assertEquals(1, response.getInt("total"));
    assertEquals("Amber", datarows.getJSONArray(0)
            .getString(getFieldIndex(response, "firstname")));
  }

  @Test
  public void notLikeTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE firstname NOT LIKE 'amb%%'",
            TEST_INDEX_ACCOUNT));

    assertNotEquals(0, response.getInt("total"));

    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      String firstname = datarows.getJSONArray(i).getString(getFieldIndex(response, "firstname"));
      assertFalse(firstname.toLowerCase().startsWith("amb"));
    }
  }

  @Test
  public void regexQueryTest() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * " +
                "FROM %s " +
                "WHERE dog_name = REGEXP_QUERY('sn.*', 'INTERSECTION|COMPLEMENT|EMPTY', 10000)",
            TestsConstants.TEST_INDEX_DOG));
    assertEquals(1, response.getInt("total"));
    verifyDataRows(response, rows("snoopy", "Hattie", 4));
  }

  @Test
  public void negativeRegexQueryTest() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * " +
                "FROM %s " +
                "WHERE NOT(dog_name = REGEXP_QUERY('sn.*', 'INTERSECTION|COMPLEMENT|EMPTY', 10000))",
            TestsConstants.TEST_INDEX_DOG));

    assertEquals(1, response.getInt("total"));
    verifyDataRows(response, rows("rex", "Daenerys", 2));
  }

  @Test
  public void doubleNotTest() {
    JSONObject response1 = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE NOT gender LIKE 'm' AND NOT gender LIKE 'f'",
            TEST_INDEX_ACCOUNT));
    assertEquals(0, response1.getInt("total"));

    JSONObject response2 = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE NOT gender LIKE 'm' AND gender NOT LIKE 'f'",
            TEST_INDEX_ACCOUNT));
    assertEquals(0, response2.getInt("total"));

    JSONObject response3 = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE gender NOT LIKE 'm' AND gender NOT LIKE 'f'",
            TEST_INDEX_ACCOUNT));
    assertEquals(0, response3.getInt("total"));

    JSONObject response4 = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE gender LIKE 'm' AND NOT gender LIKE 'f'",
            TEST_INDEX_ACCOUNT));
    // Assert there are results and they all have gender 'm'
    assertNotEquals(0, response4.getInt("total"));
    JSONArray datarows = response4.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);
      assertEquals("m",
              hit.getString(getFieldIndex(response4, "gender")).toLowerCase());
    }

    JSONObject response5 = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE NOT (gender = 'm' OR gender = 'f')",
            TEST_INDEX_ACCOUNT));
    assertEquals(0, response5.getInt("total"));
  }

  @Test
  public void limitTest() {
    JSONObject response = executeJdbcRequest(String.format("SELECT * FROM %s LIMIT 30",
        TEST_INDEX_ACCOUNT));

    assertEquals(30, response.getInt("total"));
  }

  @Test
  public void betweenTest() {
    int min = 27;
    int max = 30;
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * FROM %s WHERE age BETWEEN %s AND %s LIMIT 1000",
            TestsConstants.TEST_INDEX_PEOPLE, min, max));

    assertEquals(1, response.getInt("total"));
    verifyDataRows(response, rows(13,"Nanette","789 Madison Street",32838,"F",
            "Nogal","Quility","VA",28,"nanettebates@quility.com","Bates"));
  }

  // TODO When using NOT BETWEEN on fields, documents not containing the field
  //  are returned as well. This may be incorrect behavior.
  @Test
  public void notBetweenTest() {
    int min = 20;
    int max = 37;
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE age NOT BETWEEN %s AND %s LIMIT 1000",
            TestsConstants.TEST_INDEX_PEOPLE, min, max));

    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);

      // Ignore documents which do not contain the age field
      int indexOfAgeField = getFieldIndex(response, "age");
      if (indexOfAgeField >= 0) {
        int age = hit.getInt(indexOfAgeField);
        assertThat(age, not(allOf(greaterThanOrEqualTo(min), lessThanOrEqualTo(max))));
      }
    }
  }

  @Test
  public void inTest() throws IOException {
    JSONObject response = executeQuery(
        String.format(Locale.ROOT, "SELECT age FROM %s WHERE age IN (20, 22) LIMIT 1000",
            TestsConstants.TEST_INDEX_PHRASE));

    JSONArray hits = getHits(response);
    for (int i = 0; i < hits.length(); i++) {
      JSONObject hit = hits.getJSONObject(i);
      int age = getSource(hit).getInt("age");
      assertThat(age, isOneOf(20, 22));
    }
  }

  @Test
  public void inTestWithStrings() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT phrase FROM %s WHERE phrase IN ('quick', 'fox') LIMIT 1000",
            TestsConstants.TEST_INDEX_PHRASE));

    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);
      String phrase = hit.getString(getFieldIndex(response, "phrase"));
      assertThat(phrase, isOneOf("quick fox here", "fox brown", "quick fox", "brown fox"));
    }
  }

  @Test
  public void inTermsTestWithIdentifiersTreatedLikeStrings() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT name " +
                "FROM %s " +
                "WHERE name.firstname = IN_TERMS('daenerys','eddard') " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(2, response.getInt("total"));
  }

  @Test
  public void inTermsTestWithStrings() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT name " +
                "FROM %s " +
                "WHERE name.firstname = IN_TERMS('daenerys','eddard') " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(2, response.getInt("total"));
  }

  @Test
  public void inTermsWithNumbers() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT name " +
                "FROM %s " +
                "WHERE name.ofHisName = IN_TERMS(4,2) " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void termQueryWithNumber() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT,
            "SELECT name FROM %s WHERE name.ofHisName = term(4) LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void termQueryWithStringIdentifier() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT name " +
                "FROM %s " +
                "WHERE name.firstname = term('brandon') " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void termQueryWithStringLiteral() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT name " +
                "FROM %s " +
                "WHERE name.firstname = term('brandon') " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(1, response.getInt("total"));
  }

  // TODO When using NOT IN on fields, documents not containing the field
  //  are returned as well. This may be incorrect behavior.
  @Test
  public void notInTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT age FROM %s WHERE age NOT IN (20, 22) LIMIT 1000",
            TestsConstants.TEST_INDEX_PEOPLE));

    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);

      // Ignore documents which do not contain the age field
      int indexOfAgeField = getFieldIndex(response, "age");
      if (indexOfAgeField >= 0) {
        int age = hit.getInt(indexOfAgeField);
        assertThat(age, not(isOneOf(20, 22)));
      }
    }
  }

  @Test
  public void dateSearch() throws IOException {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(TestsConstants.DATE_FORMAT);
    DateTime dateToCompare = new DateTime(2014, 8, 18, 0, 0, 0);

    // When run using executeJdbcRequest(),fails with
    // 'timestamp:2014-08-18 in unsupported format, please use yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'
    JSONObject response = executeQuery(
        String.format(Locale.ROOT, "SELECT insert_time FROM %s WHERE insert_time < '2014-08-18'",
            TestsConstants.TEST_INDEX_ONLINE));
    JSONArray hits = getHits(response);
    for (int i = 0; i < hits.length(); i++) {
      JSONObject hit = hits.getJSONObject(i);
      JSONObject source = getSource(hit);
      DateTime insertTime = formatter.parseDateTime(source.getString("insert_time"));

      String errorMessage =
          String.format(Locale.ROOT, "insert_time must be before 2014-08-18. Found: %s",
              insertTime);
      Assert.assertTrue(errorMessage, insertTime.isBefore(dateToCompare));
    }
  }

  @Test
  public void dateSearchBraces() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT,
            "SELECT odbc_time FROM %s WHERE odbc_time < {ts '2015-03-15 00:00:00.000'}",
            TestsConstants.TEST_INDEX_ODBC));
    assertEquals(8, response.getInt("total"));
    verifyDataRows(response,
            rows("2015-03-14 13:27:33.953"), rows("2015-03-13 13:27:33.954"),
            rows("2015-03-12 13:27:33.954"), rows("2015-03-11 13:27:33.955"),
            rows("2015-03-10 13:27:33.955"), rows("2015-03-09 13:27:33.955"),
            rows("2015-03-08 13:27:33.956"), rows("2015-03-07 13:27:33.956"));
  }

  @Test
  public void dateBetweenSearch() throws IOException {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(TestsConstants.DATE_FORMAT);

    DateTime dateLimit1 = new DateTime(2014, 8, 18, 0, 0, 0);
    DateTime dateLimit2 = new DateTime(2014, 8, 21, 0, 0, 0);

    //In new engine: 'timestamp:2014-08-18 in unsupported format, please use yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'
    JSONObject response = executeQuery(
        String.format(Locale.ROOT, "SELECT insert_time " +
                "FROM %s " +
                "WHERE insert_time BETWEEN '2014-08-18' AND '2014-08-21' " +
                "LIMIT 3",
            TestsConstants.TEST_INDEX_ONLINE));
    JSONArray hits = getHits(response);
    for (int i = 0; i < hits.length(); i++) {
      JSONObject hit = hits.getJSONObject(i);
      JSONObject source = getSource(hit);
      DateTime insertTime = formatter.parseDateTime(source.getString("insert_time"));

      boolean isBetween = (insertTime.isAfter(dateLimit1) || insertTime.isEqual(dateLimit1)) &&
          (insertTime.isBefore(dateLimit2) || insertTime.isEqual(dateLimit2));

      Assert.assertTrue("insert_time must be between 2014-08-18 and 2014-08-21", isBetween);
    }
  }

  @Test
  public void missFilterSearch() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * FROM %s WHERE insert_time2 IS missing",
            TestsConstants.TEST_INDEX_PHRASE));

    assertEquals(4, response.getInt("total"));
    verifyDataRows(response,
            rows(null, "quick fox", null), rows(null, "brown fox", null),
            rows(5, "my test", null), rows(7, "my test 2", null));
  }

  @Test
  public void notMissFilterSearch() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * FROM %s WHERE insert_time2 IS NOT missing",
            TestsConstants.TEST_INDEX_PHRASE));

    assertEquals(2, response.getInt("total"));
    verifyDataRows(response,
            rows(null, "quick fox here", "2014-08-19 07:09:13.434"),
            rows(null, "fox brown", "2014-08-19 07:09:13.434"));
  }

  @Test
  public void complexConditionQuery() {
    String errorMessage = "Result does not exist to the condition " +
        "(gender='m' AND (age> 25 OR account_number>5)) OR (gender='f' AND (age>30 OR account_number < 8)";

    JSONObject response = executeJdbcRequest(
        String.format("SELECT * " +
                "FROM %s " +
                "WHERE (gender='m' AND (age> 25 OR account_number>5)) " +
                "OR (gender='f' AND (age>30 OR account_number < 8))",
            TEST_INDEX_ACCOUNT));

    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);

      String gender = hit.getString(getFieldIndex(response, "gender")).toLowerCase();
      int age = hit.getInt(getFieldIndex(response, "age"));
      int accountNumber = hit.getInt(getFieldIndex(response,"account_number"));

      assertTrue(errorMessage,
          (gender.equals("m") && (age > 25 || accountNumber > 5))
              || (gender.equals("f") && (age > 30 || accountNumber < 8)));
    }
  }

  @Test
  public void complexNotConditionQuery() {
    String errorMessage = "Result does not exist to the condition " +
        "NOT (gender='m' AND NOT (age > 25 OR account_number > 5)) " +
        "OR (NOT gender='f' AND NOT (age > 30 OR account_number < 8))";

    JSONObject response = executeJdbcRequest(
        String.format("SELECT * " +
                "FROM %s " +
                "WHERE NOT (gender='m' AND NOT (age > 25 OR account_number > 5)) " +
                "OR (NOT gender='f' AND NOT (age > 30 OR account_number < 8))",
            TEST_INDEX_ACCOUNT));

    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);

      String gender = hit.getString(getFieldIndex(response, "gender")).toLowerCase();
      int age = hit.getInt(getFieldIndex(response, "age"));
      int accountNumber = hit.getInt(getFieldIndex(response, "account_number"));

      assertFalse(errorMessage, gender.equals("m") && !(age > 25 || accountNumber > 5));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void orderByAscTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT age FROM %s ORDER BY age ASC LIMIT 1000",
            TEST_INDEX_ACCOUNT));

    ArrayList<Integer> ages = new ArrayList<>();
    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);

      ages.add(hit.getInt(getFieldIndex(response, "age")));
    }

    ArrayList<Integer> sortedAges = (ArrayList<Integer>) ages.clone();
    Collections.sort(sortedAges);
    assertEquals("The list is not in ascending order", sortedAges, ages);
  }

  @Test
  public void orderByDescTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT age FROM %s ORDER BY age DESC LIMIT 1000",
            TEST_INDEX_ACCOUNT));
    assertResponseForOrderByTest(response);
  }

  @Test
  public void orderByDescUsingTableAliasTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT a.age FROM %s a ORDER BY a.age DESC LIMIT 1000",
            TEST_INDEX_ACCOUNT));
    assertResponseForOrderByTest(response);
  }

  @SuppressWarnings("unchecked")
  private void assertResponseForOrderByTest(JSONObject response) {
    ArrayList<Integer> ages = new ArrayList<>();
    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);

      ages.add(hit.getInt(getFieldIndex(response, "age")));
    }

    ArrayList<Integer> sortedAges = (ArrayList<Integer>) ages.clone();
    Collections.sort(sortedAges, Collections.reverseOrder());
    assertEquals("The list is not in ascending order", sortedAges, ages);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void orderByAscFieldWithSpaceTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * " +
                "FROM %s " +
                "WHERE `test field` IS NOT null " +
                "ORDER BY `test field` ASC " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_PHRASE));

    ArrayList<Integer> testFields = new ArrayList<>();
    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);

      testFields.add(hit.getInt(getFieldIndex(response, "test field")));
    }

    ArrayList<Integer> sortedTestFields = (ArrayList<Integer>) testFields.clone();
    Collections.sort(sortedTestFields);
    assertEquals("The list is not in ascending order", sortedTestFields, testFields);
  }

  @Test
  public void testWhereWithBoolEqualsTrue() {
    JSONObject response = executeJdbcRequest(
        StringUtils.format(
            "SELECT * " +
                "FROM %s " +
                "WHERE male = true " +
                "LIMIT 5",
            TestsConstants.TEST_INDEX_BANK)
    );

    assertEquals(BANK_INDEX_MALE_TRUE, response.getInt("total"));
  }

  @Test
  public void testWhereWithBoolEqualsTrueAndGroupBy() {
    JSONObject response = executeJdbcRequest(
        StringUtils.format(
            "SELECT * " +
                "FROM %s " +
                "WHERE male = true " +
                "GROUP BY balance " +
                "LIMIT 5",
            TestsConstants.TEST_INDEX_BANK)
    );

    assertEquals(BANK_INDEX_MALE_TRUE, response.getInt("total"));
  }

  @Test
  public void testWhereWithBoolEqualsTrueAndOrderBy() {
    JSONObject response = executeJdbcRequest(
        StringUtils.format(
            "SELECT * " +
                "FROM %s " +
                "WHERE male = true " +
                "ORDER BY age " +
                "LIMIT 5",
            TestsConstants.TEST_INDEX_BANK)
    );

    assertEquals(BANK_INDEX_MALE_TRUE, response.getInt("total"));
  }

  @Test
  public void testWhereWithBoolIsTrue() {
    JSONObject response = executeJdbcRequest(
        StringUtils.format(
            "SELECT * " +
                "FROM %s " +
                "WHERE male IS true " +
                "GROUP BY balance " +
                "LIMIT 5",
            TestsConstants.TEST_INDEX_BANK)
    );

    assertEquals(4, response.getInt("total"));
    verifyDataRows(response,
            rows(4180), rows(5686),
            rows(16418), rows(39225));
  }

  @Test
  public void testWhereWithBoolIsNotTrue() {
    JSONObject response = executeJdbcRequest(
        StringUtils.format(
            "SELECT * " +
                "FROM %s " +
                "WHERE male IS NOT true " +
                "GROUP BY balance " +
                "LIMIT 5",
            TestsConstants.TEST_INDEX_BANK)
    );

    assertEquals(3, response.getInt("total"));
    verifyDataRows(response, rows(32838),
            rows(40540), rows(48086));
  }

  @Test
  public void testWhereWithBoolEqualsFalse() {
    JSONObject response = executeJdbcRequest(
        StringUtils.format(
            "SELECT * " +
                "FROM %s " +
                "WHERE male = false " +
                "LIMIT 5",
            TestsConstants.TEST_INDEX_BANK)
    );

    assertEquals(3, response.getInt("total"));
  }

  @Test
  public void testWhereWithBoolEqualsFalseAndGroupBy() {
    JSONObject response = executeJdbcRequest(
        StringUtils.format(
            "SELECT * " +
                "FROM %s " +
                "WHERE male = false " +
                "GROUP BY balance " +
                "LIMIT 5",
            TestsConstants.TEST_INDEX_BANK)
    );

    assertEquals(3, response.getInt("total"));
    verifyDataRows(response, rows(32838),
            rows(40540), rows(48086));
  }

  @Test
  public void testWhereWithBoolEqualsFalseAndOrderBy() {
    JSONObject response = executeJdbcRequest(
        StringUtils.format(
            "SELECT * " +
                "FROM %s " +
                "WHERE male = false " +
                "ORDER BY age " +
                "LIMIT 5",
            TestsConstants.TEST_INDEX_BANK)
    );

    assertEquals(3, response.getInt("total"));
  }

  @Test
  public void testWhereWithBoolIsFalse() {
    JSONObject response = executeJdbcRequest(
        StringUtils.format(
            "SELECT * " +
                "FROM %s " +
                "WHERE male IS false " +
                "GROUP BY balance " +
                "LIMIT 5",
            TestsConstants.TEST_INDEX_BANK)
    );

    assertEquals(3, response.getInt("total"));
    verifyDataRows(response, rows(32838),
            rows(40540), rows(48086));
  }

  @Test
  public void testWhereWithBoolIsNotFalse() {
    JSONObject response = executeJdbcRequest(
        StringUtils.format(
            "SELECT * " +
                "FROM %s " +
                "WHERE male IS NOT false " +
                "GROUP BY balance " +
                "LIMIT 5",
            TestsConstants.TEST_INDEX_BANK)
    );

    assertEquals(4, response.getInt("total"));
    verifyDataRows(response,
            rows(4180), rows(5686),
            rows(16418), rows(39225));
  }

  @Test
  public void testMultiPartWhere() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * " +
                "FROM %s " +
                "WHERE (firstname LIKE 'opal' OR firstname LIKE 'rodriquez') " +
                "AND (state like 'oh' OR state like 'hi')",
            TEST_INDEX_ACCOUNT));

    assertEquals(2, response.getInt("total"));
  }

  @Test
  public void testMultiPartWhere2() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * " +
                "FROM %s " +
                "WHERE ((account_number > 200 AND account_number < 300) OR gender LIKE 'm') " +
                "AND (state LIKE 'hi' OR address LIKE 'avenue')",
            TEST_INDEX_ACCOUNT));

    assertEquals(127, response.getInt("total"));
  }

  @Test
  public void testMultiPartWhere3() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * " +
                "FROM %s " +
                "WHERE ((account_number > 25 AND account_number < 75) AND age >35 ) " +
                "AND (state LIKE 'md' OR (address LIKE 'avenue' OR address LIKE 'street'))",
            TEST_INDEX_ACCOUNT));

    Assert.assertEquals(7, response.getInt("total"));
  }

  @Test
  public void filterPolygonTest() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * " +
                "FROM %s " +
                "WHERE GEO_INTERSECTS(place,'POLYGON ((102 2, 103 2, 103 3, 102 3, 102 2))')",
            TestsConstants.TEST_INDEX_LOCATION));

    assertEquals(1, response.getInt("total"));
    verifyDataRows(response, rows("bigSquare"));
  }

  @Test
  public void boundingBox() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT,
            "SELECT * FROM %s WHERE GEO_BOUNDING_BOX(center, 100.0, 1.0, 101, 0.0)",
            TestsConstants.TEST_INDEX_LOCATION));

    assertEquals(1, response.getInt("total"));
    verifyDataRows(response, rows("square"));
  }

  @Test
  public void geoDistance() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT,
            "SELECT * FROM %s WHERE GEO_DISTANCE(center, '1km', 100.5, 0.500001)",
            TestsConstants.TEST_INDEX_LOCATION));

    assertEquals(1, response.getInt("total"));
    verifyDataRows(response, rows("square"));
  }

  @Test
  public void geoPolygon() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT,
            "SELECT * FROM %s WHERE GEO_POLYGON(center, 100,0, 100.5, 2, 101.0,0)",
            TestsConstants.TEST_INDEX_LOCATION));

    assertEquals(1, response.getInt("total"));
    verifyDataRows(response, rows("square"));
  }

  @Ignore
  @Test
  public void escapedCharactersCheck() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * " +
                "FROM %s " +
                "WHERE MATCH_PHRASE(nickname, 'Daenerys \"Stormborn\"') " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void complexObjectSearch() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * " +
                "FROM %s " +
                "WHERE MATCH_PHRASE(name.firstname, 'Jaime') " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void complexObjectReturnField() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT parents.father " +
                "FROM %s " +
                "WHERE MATCH_PHRASE(name.firstname, 'Brandon') " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    Assert.assertEquals(1, response.getInt("total"));

    JSONArray datarows = response.getJSONArray("datarows");
    JSONArray hit = datarows.getJSONArray(0);
    assertEquals("Eddard", hit.get(0));
  }

  /**
   * TODO: Fields prefixed with @ gets converted to SQLVariantRefExpr instead of SQLIdentifierExpr
   * Either change SQLVariantRefExpr to SQLIdentifierExpr
   * Or handle the special case for SQLVariantRefExpr
   */
  @Ignore
  @Test
  public void queryWithAtFieldOnWhere() {
    JSONObject response = executeJdbcRequest(String.format(
        "SELECT * FROM %s where @wolf = 'Summer' LIMIT 1000", TEST_INDEX_GAME_OF_THRONES));
    assertEquals(1, response.getInt("total"));
    JSONArray datarows = response.getJSONArray("datarows");
    JSONArray hit = datarows.getJSONArray(0);
    assertEquals("Summer", hit.get(0));
//    Assert.assertEquals("Brandon", hit.query("name/firstname"));
  }

  @Test
  public void queryWithDotAtStartOfIndexName() throws Exception {
    TestUtils.createHiddenIndexByRestClient(client(), ".bank", null);
    TestUtils.loadDataByRestClient(client(), ".bank", "/src/test/resources/.bank.json");

    String response = executeQuery("SELECT education FROM .bank WHERE account_number = 12345",
        "jdbc");
    Assert.assertTrue(response.contains("PhD"));
  }

  @Ignore("Fails due to SearchPhaseExecutionException")
  @Test
  public void notLikeTests() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT name " +
                "FROM %s " +
                "WHERE name.firstname NOT LIKE 'd%%' AND name IS NOT NULL " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    Assert.assertEquals(3, response.getInt("total"));
    JSONArray datarows = response.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);

      String name = hit.getString(getFieldIndex(response, "firstname"));
      assertFalse(String.format("Name [%s] should not match pattern [d%%]", name),
              name.startsWith("d"));
    }
  }

  @Test
  public void isNullTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT name " +
                "FROM %s " +
                "WHERE nickname IS NULL " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(6, response.getInt("total"));
  }

  @Test
  public void isNotNullTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT name " +
                "FROM %s " +
                "WHERE nickname IS NOT NULL " +
                "LIMIT 1000",
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void innerQueryTest() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * " +
                "FROM %s D " +
                "WHERE holdersName IN (SELECT firstname " +
                "FROM %s " +
                "WHERE firstname = 'Hattie')",
            TestsConstants.TEST_INDEX_DOG, TEST_INDEX_ACCOUNT));

    assertEquals(1, response.getInt("total"));
    verifyDataRows(response, rows("snoopy", "Hattie", 4));
  }

  @Ignore
  @Test
  public void twoSubQueriesTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * " +
                "FROM %s " +
                "WHERE holdersName IN (SELECT firstname " +
                "FROM %s " +
                "WHERE firstname = 'Hattie') " +
                "AND age IN (SELECT name.ofHisName " +
                "FROM %s " +
                "WHERE name.firstname <> 'Daenerys' " +
                "AND name.ofHisName IS NOT NULL) ",
            TestsConstants.TEST_INDEX_DOG,
            TEST_INDEX_ACCOUNT,
            TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(1, response.getInt("total"));

    JSONArray datarows = response.getJSONArray("datarows");
    JSONArray hit = datarows.getJSONArray(0);
    assertEquals("snoopy", hit.getString(getFieldIndex(response, "dog_name")));
    assertEquals("Hattie", hit.getString(getFieldIndex(response, "holdersName")));
    assertEquals(4, hit.getInt(getFieldIndex(response,"age")));
  }

  @Ignore
  @Test
  public void inTermsSubQueryTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * " +
                "FROM %s " +
                "WHERE age = IN_TERMS (SELECT name.ofHisName " +
                "FROM %s " +
                "WHERE name.firstname <> 'Daenerys' " +
                "AND name.ofHisName IS NOT NULL)",
            TestsConstants.TEST_INDEX_DOG, TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(1, response.getInt("total"));

    JSONArray datarows = response.getJSONArray("datarows");
    JSONArray hit = datarows.getJSONArray(0);
    assertEquals("snoopy", hit.getString(getFieldIndex(response, "dog_name")));
    assertEquals("Hattie", hit.getString(getFieldIndex(response, "holdersName")));
    assertEquals(4, hit.getInt(getFieldIndex(response,"age")));
  }

  @Ignore
  @Test
  public void idsQueryOneId() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE _id = IDS_QUERY(dog, 1)",
            TestsConstants.TEST_INDEX_DOG));

    assertEquals(1, response.getInt("total"));

    JSONArray datarows = response.getJSONArray("datarows");
    JSONArray hit = datarows.getJSONArray(0);
    assertEquals("rex", hit.getString(getFieldIndex(response, "dog_name")));
    assertEquals("Daenerys", hit.getString(getFieldIndex(response, "holdersName")));
    assertEquals(2, hit.getInt(getFieldIndex(response, "age")));
  }

  @Ignore
  @Test
  public void idsQueryMultipleId() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * FROM %s WHERE _id = IDS_QUERY(dog, 1, 2, 3)",
            TestsConstants.TEST_INDEX_DOG));

    assertEquals(1, response.getInt("total"));

    JSONArray datarows = response.getJSONArray("datarows");
    JSONArray hit = datarows.getJSONArray(0);
    assertEquals("rex", hit.getString(getFieldIndex(response, "dog_name")));
    assertEquals("Daenerys", hit.getString(getFieldIndex(response, "holdersName")));
    assertEquals(2, hit.getInt(getFieldIndex(response, "age")));
  }

  @Ignore
  @Test
  public void idsQuerySubQueryIds() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT * " +
                "FROM %s " +
                "WHERE _id = IDS_QUERY(dog, (SELECT name.ofHisName " +
                "FROM %s " +
                "WHERE name.firstname <> 'Daenerys' " +
                "AND name.ofHisName IS NOT NULL))",
            TestsConstants.TEST_INDEX_DOG, TestsConstants.TEST_INDEX_GAME_OF_THRONES));

    assertEquals(1, response.getInt("total"));

    JSONArray datarows = response.getJSONArray("datarows");
    JSONArray hit = datarows.getJSONArray(0);
    assertEquals("rex", hit.getString(getFieldIndex(response, "dog_name")));
    assertEquals("Daenerys", hit.getString(getFieldIndex(response, "holdersName")));
    assertEquals(2, hit.getInt(getFieldIndex(response, "age")));
  }

  @Test
  public void nestedEqualsTestFieldNormalField() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * FROM %s WHERE nested(message.info)='b'",
            TestsConstants.TEST_INDEX_NESTED_TYPE));

    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void nestedEqualsTestFieldInsideArrays() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * FROM %s WHERE nested(message.info) = 'a'",
            TestsConstants.TEST_INDEX_NESTED_TYPE));

    assertEquals(2, response.getInt("total"));
  }

  @Ignore // Seems like we don't support nested with IN, throwing IllegalArgumentException
  @Test
  public void nestedOnInQuery() {
    JSONObject response = executeJdbcRequest(String.format(
        "SELECT * FROM %s where nested(message.info) IN ('a','b')", TEST_INDEX_NESTED_TYPE));
    assertEquals(3, response.getInt("total"));
  }

  @Test
  public void complexNestedQueryBothOnSameObject() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * " +
                "FROM %s " +
                "WHERE nested('message', message.info = 'a' AND message.author ='i')",
            TestsConstants.TEST_INDEX_NESTED_TYPE));

    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void complexNestedQueryNotBothOnSameObject() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * " +
                "FROM %s " +
                "WHERE nested('message', message.info = 'a' AND message.author ='h')",
            TestsConstants.TEST_INDEX_NESTED_TYPE));

    verifySchema(response,
            schema("myNum", null, "long"),
            schema("someField", null, "keyword"),
            schema("comment", null, "text"),
            schema("message", null, "text"));
  }

  @Test
  public void nestedOnInTermsQuery() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT, "SELECT * " +
                "FROM %s " +
                "WHERE nested(message.info) = IN_TERMS('a', 'b')",
            TestsConstants.TEST_INDEX_NESTED_TYPE));

    assertEquals(3, response.getInt("total"));
  }

  // TODO Uncomment these after problem with loading join index is resolved
//    @Test
//    public void childrenEqualsTestFieldNormalField() throws IOException {
//        JSONObject response = executeQuery(
//                        String.format(Locale.ROOT, "SELECT * " +
//                                      "FROM %s/joinType " +
//                                      "WHERE children(childrenType, info) = 'b'", TestsConstants.TEST_INDEX_JOIN_TYPE));
//
//        Assert.assertEquals(1, getTotalHits(response));
//    }
//
//    @Test
//    public void childrenOnInQuery() throws IOException {
//        JSONObject response = executeQuery(
//                        String.format(Locale.ROOT, "SELECT * " +
//                                      "FROM %s/joinType " +
//                                      "WHERE children(childrenType, info) IN ('a', 'b')",
//                                TestsConstants.TEST_INDEX_JOIN_TYPE));
//
//        Assert.assertEquals(2, getTotalHits(response));
//    }
//
//    @Test
//    public void complexChildrenQueryBothOnSameObject() throws IOException {
//        JSONObject response = executeQuery(
//                        String.format(Locale.ROOT, "SELECT * " +
//                                      "FROM %s/joinType " +
//                                      "WHERE children(childrenType, info = 'a' AND author ='e')",
//                                TestsConstants.TEST_INDEX_JOIN_TYPE));
//
//        Assert.assertEquals(1, getTotalHits(response));
//    }
//
//    @Test
//    public void complexChildrenQueryNotOnSameObject() throws IOException {
//        JSONObject response = executeQuery(
//                        String.format(Locale.ROOT, "SELECT * " +
//                                      "FROM %s/joinType " +
//                                      "WHERE children(childrenType, info = 'a' AND author ='j')",
//                                TestsConstants.TEST_INDEX_JOIN_TYPE));
//
//        Assert.assertEquals(0, getTotalHits(response));
//    }
//
//    @Test
//    public void childrenOnInTermsQuery() throws IOException {
//        JSONObject response = executeQuery(
//                        String.format(Locale.ROOT, "SELECT * " +
//                                      "FROM %s/joinType " +
//                                      "WHERE children(childrenType, info) = IN_TERMS(a, b)",
//                                TestsConstants.TEST_INDEX_JOIN_TYPE));
//
//        Assert.assertEquals(2, getTotalHits(response));
//    }

  @Ignore // the hint does not really work, NoSuchIndexException is thrown
  @Test
  public void multipleIndicesOneNotExistWithHint(){

    JSONObject response = executeJdbcRequest(String
        .format("SELECT /*! IGNORE_UNAVAILABLE */ * FROM %s,%s ", TEST_INDEX_ACCOUNT,
            "badindex"));

    assertTrue(response.getInt("total") > 0);
  }

  @Test
  public void multipleIndicesOneNotExistWithoutHint() throws IOException {
    try {
      executeQuery(
          String.format(Locale.ROOT, "SELECT * FROM %s, %s", TEST_INDEX_ACCOUNT, "badindex"));
      Assert.fail("Expected exception, but call succeeded");
    } catch (ResponseException e) {
      Assert.assertEquals(RestStatus.BAD_REQUEST.getStatus(),
          e.getResponse().getStatusLine().getStatusCode());
      final String entity = TestUtils.getResponseBody(e.getResponse());
      Assert.assertThat(entity, containsString("\"type\": \"IndexNotFoundException\""));
    }
  }

  // TODO Find way to check routing() without SearchRequestBuilder
  //  to properly update these tests to OpenSearchIntegTestCase format
//    @Test
//    public void routingRequestOneRounting() throws IOException {
//        SqlElasticSearchRequestBuilder request = getRequestBuilder(String.format(Locale.ROOT,
//                                  "SELECT /*! ROUTINGS(hey) */ * FROM %s ", TEST_INDEX_ACCOUNT));
//        SearchRequestBuilder searchRequestBuilder = (SearchRequestBuilder) request.getBuilder();
//        Assert.assertEquals("hey",searchRequestBuilder.request().routing());
//    }
//
//    @Test
//    public void routingRequestMultipleRountings() throws IOException {
//        SqlElasticSearchRequestBuilder request = getRequestBuilder(String.format(Locale.ROOT,
//                                  "SELECT /*! ROUTINGS(hey,bye) */ * FROM %s ", TEST_INDEX_ACCOUNT));
//        SearchRequestBuilder searchRequestBuilder = (SearchRequestBuilder) request.getBuilder();
//        Assert.assertEquals("hey,bye",searchRequestBuilder.request().routing());
//    }

  @Ignore // Getting parser error: syntax error, expect RPAREN, actual IDENTIFIER insert_time
  @Test
  public void scriptFilterNoParams() {

    JSONObject result = executeJdbcRequest(String.format(
        "SELECT insert_time FROM %s where script('doc[\\'insert_time\''].date.hourOfDay==16') " +
            "and insert_time <'2014-08-21T00:00:00.000Z'", TEST_INDEX_ONLINE));
    assertEquals(237, result.getInt("total"));
  }

  @Ignore // Getting parser error: syntax error, expect RPAREN, actual IDENTIFIER insert_time
  @Test
  public void scriptFilterWithParams() {

    JSONObject result = executeJdbcRequest(String.format(
        "SELECT insert_time FROM %s where script('doc[\\'insert_time\''].date.hourOfDay==x','x'=16) " +
            "and insert_time <'2014-08-21T00:00:00.000Z'", TEST_INDEX_ONLINE));
    assertEquals(237, result.getInt("total"));
  }

  @Test
  public void highlightPreTagsAndPostTags() {
    JSONObject response = executeJdbcRequest(
        String.format(Locale.ROOT,
            "SELECT /*! HIGHLIGHT(phrase, pre_tags : ['<b>'], post_tags : ['</b>']) */ " +
                "* FROM %s " +
                "WHERE phrase LIKE 'fox' " +
                "ORDER BY _score", TestsConstants.TEST_INDEX_PHRASE));

    assertEquals(4, response.getInt("total"));
    verifyDataRows(response,
            rows(null, "quick fox", null),
            rows(null, "quick fox here", "2014-08-19 07:09:13.434"),
            rows(null, "brown fox", null),
            rows(null, "fox brown", "2014-08-19 07:09:13.434"));
  }

  @Ignore
  @Test
  public void fieldCollapsingTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT /*! COLLAPSE({\"field\":\"age\"," +
            "\"inner_hits\":{\"name\": \"account\"," +
            "\"size\":1," +
            "\"sort\":[{\"age\":\"asc\"}]}," +
            "\"max_concurrent_group_searches\": 4}) */ " +
            "* FROM %s", TEST_INDEX_ACCOUNT));


    assertEquals(21, response.getInt("total"));
  }

  @Ignore("New engine doesn't have 'alias' field in schema in response")
  @Test
  public void backticksQuotedIndexNameTest() throws Exception {
    TestUtils.createIndexByRestClient(client(), "bank_unquote", null);
    TestUtils
        .loadDataByRestClient(client(), "bank", "/src/test/resources/bank_for_unquote_test.json");

    JSONArray hits = getHits(executeQuery("SELECT lastname FROM `bank`"));
    Object responseIndex = ((JSONObject) hits.get(0)).query("/_index");
    assertEquals("bank", responseIndex);

    assertEquals(
        executeQuery("SELECT lastname FROM bank", "jdbc"),
        executeQuery("SELECT `bank`.`lastname` FROM `bank`", "jdbc")
    );

    assertEquals(
        executeQuery(
            "SELECT `b`.`age` AS `AGE`, AVG(`b`.`balance`) FROM `bank` AS `b` " +
                "WHERE ABS(`b`.`age`) > 20 GROUP BY `b`.`age` ORDER BY `b`.`age`",
            "jdbc"),
        executeQuery("SELECT b.age AS AGE, AVG(balance) FROM bank AS b " +
                "WHERE ABS(age) > 20 GROUP BY b.age ORDER BY b.age",
            "jdbc")
    );
  }

  @Test
  public void backticksQuotedFieldNamesTest() {
    String expected = executeQuery(StringUtils.format("SELECT b.lastname FROM %s " +
        "AS b ORDER BY age LIMIT 3", TestsConstants.TEST_INDEX_BANK), "jdbc");
    String quotedFieldResult = executeQuery(StringUtils.format("SELECT b.`lastname` FROM %s " +
        "AS b ORDER BY age LIMIT 3", TestsConstants.TEST_INDEX_BANK), "jdbc");

    assertEquals(expected, quotedFieldResult);
  }

  @Test
  public void backticksQuotedAliasTest() {
    String expected = executeQuery(StringUtils.format("SELECT b.lastname FROM %s " +
        "AS b ORDER BY age LIMIT 3", TestsConstants.TEST_INDEX_BANK), "jdbc");
    String quotedAliasResult = executeQuery(StringUtils.format("SELECT `b`.lastname FROM %s" +
        " AS `b` ORDER BY age LIMIT 3", TestsConstants.TEST_INDEX_BANK), "jdbc");
    String quotedAliasAndFieldResult =
        executeQuery(StringUtils.format("SELECT `b`.`lastname` FROM %s " +
            "AS `b` ORDER BY age LIMIT 3", TestsConstants.TEST_INDEX_BANK), "jdbc");

    assertEquals(expected, quotedAliasResult);
    assertEquals(expected, quotedAliasAndFieldResult);
  }

  @Test
  public void backticksQuotedAliasWithSpecialCharactersTest() {
    String expected = executeQuery(StringUtils.format("SELECT b.lastname FROM %s " +
        "AS b ORDER BY age LIMIT 3", TestsConstants.TEST_INDEX_BANK), "jdbc");
    String specialCharAliasResult =
        executeQuery(StringUtils.format("SELECT `b k`.lastname FROM %s " +
            "AS `b k` ORDER BY age LIMIT 3", TestsConstants.TEST_INDEX_BANK), "jdbc");

    assertEquals(expected, specialCharAliasResult);
  }

  @Test
  public void backticksQuotedAliasInJDBCResponseTest() {
    String query = StringUtils.format("SELECT `b`.`lastname` AS `name` FROM %s AS `b` " +
        "ORDER BY age LIMIT 3", TestsConstants.TEST_INDEX_BANK);
    String response = executeQuery(query, "jdbc");

    assertTrue(response.contains("\"alias\": \"name\""));
  }

  @Test
  public void caseWhenSwitchTest() {
    JSONObject response = executeJdbcRequest("SELECT CASE age " +
        "WHEN 30 THEN '1' " +
        "WHEN 40 THEN '2' " +
        "ELSE '0' END AS cases FROM " + TEST_INDEX_ACCOUNT + " WHERE age IS NOT NULL");

    assertEquals(1000, response.getInt("total"));
  }

  @Test
  public void caseWhenJdbcResponseTest() {
    String response = executeQuery("SELECT CASE age " +
        "WHEN 30 THEN 'age is 30' " +
        "WHEN 40 THEN 'age is 40' " +
        "ELSE 'NA' END AS cases FROM " + TEST_INDEX_ACCOUNT + " WHERE age is not null", "jdbc");
    assertTrue(
        response.contains("age is 30") ||
            response.contains("age is 40") ||
            response.contains("NA")
    );
  }

  @Ignore("This is already supported in new SQL engine")
  @Test
  public void functionInCaseFieldShouldThrowESExceptionDueToIllegalScriptInJdbc() {
    String response = executeQuery(
        "select case lower(firstname) when 'amber' then '1' else '2' end as cases from " +
            TEST_INDEX_ACCOUNT,
        "jdbc");
    queryInJdbcResponseShouldIndicateESException(response, "SearchPhaseExecutionException",
        "For more details, please send request for Json format");
  }

  @Ignore("This is already supported in our new query engine")
  @Test
  public void functionCallWithIllegalScriptShouldThrowESExceptionInJdbc() {
    String response = executeQuery("select log(balance + 2) from " + TEST_INDEX_BANK,
        "jdbc");
    queryInJdbcResponseShouldIndicateESException(response, "SearchPhaseExecutionException",
        "please send request for Json format to see the raw response from OpenSearch engine.");
  }

  @Ignore("Goes in different route, does not call PrettyFormatRestExecutor.execute methods." +
      "The performRequest method in RestClient doesn't throw any exceptions for null value fields in script")
  @Test
  public void functionArgWithNullValueFieldShouldThrowESExceptionInJdbc() {
    String response = executeQuery(
        "select log(balance) from " + TEST_INDEX_BANK_WITH_NULL_VALUES, "jdbc");
    queryInJdbcResponseShouldIndicateESException(response, "SearchPhaseExecutionException",
        "For more details, please send request for Json format");
  }

  private void queryInJdbcResponseShouldIndicateESException(String response, String exceptionType,
                                                            String... errMsgs) {
    Assert.assertThat(response, containsString(exceptionType));
    for (String errMsg : errMsgs) {
      Assert.assertThat(response, containsString(errMsg));
    }
  }

  private void checkSelectAllAndFieldResponseSize(JSONObject response) {
    String[] arr =
        new String[] {"account_number", "firstname", "address", "birthdate", "gender", "city",
            "lastname",
            "balance", "employer", "state", "age", "email", "male"};
    Set<String> expectedSource = new HashSet<>(Arrays.asList(arr));

    JSONArray hits = getHits(response);
    Assert.assertTrue(hits.length() > 0);
    for (int i = 0; i < hits.length(); ++i) {
      JSONObject hit = hits.getJSONObject(i);
      Assert.assertEquals(expectedSource, getSource(hit).keySet());
    }
  }

  private void checkSelectAllAndFieldAggregationResponseSize(JSONObject response, String field) {
    JSONObject fieldAgg = (response.getJSONObject("aggregations")).getJSONObject(field);
    JSONArray buckets = fieldAgg.getJSONArray("buckets");
    Assert.assertTrue(buckets.length() == 6);
  }
}
