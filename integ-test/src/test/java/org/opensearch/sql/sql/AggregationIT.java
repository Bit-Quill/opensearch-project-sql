/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NULL_MISSING;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySome;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class AggregationIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.NULL_MISSING);
    loadIndex(Index.CALCS);
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.GAME_OF_THRONES);
    loadIndex(Index.DOG);
    loadIndex(Index.ONLINE);
    loadIndex(Index.NESTED);
  }

  @Test
  public void testFilteredAggregatePushDown() throws IOException {
    JSONObject response = executeQuery(
        "SELECT COUNT(*) FILTER(WHERE age > 35) FROM " + TEST_INDEX_BANK);
    verifySchema(response, schema("COUNT(*) FILTER(WHERE age > 35)", null, "integer"));
    verifyDataRows(response, rows(3));
  }

  @Test
  public void testFilteredAggregateNotPushDown() throws IOException {
    JSONObject response = executeQuery(
        "SELECT COUNT(*) FILTER(WHERE age > 35) FROM (SELECT * FROM " + TEST_INDEX_BANK
            + ") AS a");
    verifySchema(response, schema("COUNT(*) FILTER(WHERE age > 35)", null, "integer"));
    verifyDataRows(response, rows(3));
  }

  @Test
  public void testPushDownAggregationOnNullValues() throws IOException {
    // OpenSearch aggregation query (MetricAggregation)
    var response = executeQuery(String.format(
        "SELECT min(`int`), max(`int`), avg(`int`), min(`dbl`), max(`dbl`), avg(`dbl`) " +
        "FROM %s WHERE `key` = 'null'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`)", null, "integer"), schema("max(`int`)", null, "integer"),
        schema("avg(`int`)", null, "double"), schema("min(`dbl`)", null, "double"),
        schema("max(`dbl`)", null, "double"), schema("avg(`dbl`)", null, "double"));
    verifyDataRows(response, rows(null, null, null, null, null, null));
  }

  @Test
  public void testPushDownAggregationOnMissingValues() throws IOException {
    // OpenSearch aggregation query (MetricAggregation)
    var response = executeQuery(String.format(
        "SELECT min(`int`), max(`int`), avg(`int`), min(`dbl`), max(`dbl`), avg(`dbl`) " +
        "FROM %s WHERE `key` = 'null'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`)", null, "integer"), schema("max(`int`)", null, "integer"),
        schema("avg(`int`)", null, "double"), schema("min(`dbl`)", null, "double"),
        schema("max(`dbl`)", null, "double"), schema("avg(`dbl`)", null, "double"));
    verifyDataRows(response, rows(null, null, null, null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnNullValues() throws IOException {
    // In-memory aggregation performed by the plugin
    var response = executeQuery(String.format("SELECT"
        + " min(`int`) over (PARTITION BY `key`), max(`int`) over (PARTITION BY `key`),"
        + " avg(`int`) over (PARTITION BY `key`), min(`dbl`) over (PARTITION BY `key`),"
        + " max(`dbl`) over (PARTITION BY `key`), avg(`dbl`) over (PARTITION BY `key`)"
        + " FROM %s WHERE `key` = 'null'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("max(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("avg(`int`) over (PARTITION BY `key`)", null, "double"),
        schema("min(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("max(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("avg(`dbl`) over (PARTITION BY `key`)", null, "double"));
    verifyDataRows(response, // 4 rows with null values
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnMissingValues() throws IOException {
    // In-memory aggregation performed by the plugin
    var response = executeQuery(String.format("SELECT"
        + " min(`int`) over (PARTITION BY `key`), max(`int`) over (PARTITION BY `key`),"
        + " avg(`int`) over (PARTITION BY `key`), min(`dbl`) over (PARTITION BY `key`),"
        + " max(`dbl`) over (PARTITION BY `key`), avg(`dbl`) over (PARTITION BY `key`)"
        + " FROM %s WHERE `key` = 'missing'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("max(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("avg(`int`) over (PARTITION BY `key`)", null, "double"),
        schema("min(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("max(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("avg(`dbl`) over (PARTITION BY `key`)", null, "double"));
    verifyDataRows(response, // 4 rows with null values
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnNullValuesReturnsNull() throws IOException {
    var response = executeQuery(String.format("SELECT "
        + " max(int0) over (PARTITION BY `datetime1`),"
        + " min(int0) over (PARTITION BY `datetime1`),"
        + " avg(int0) over (PARTITION BY `datetime1`)"
        + "from %s where int0 IS NULL;", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("min(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("avg(int0) over (PARTITION BY `datetime1`)", null, "double"));
    verifySome(response.getJSONArray("datarows"), rows(null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnAllValuesAndOnNotNullReturnsSameResult() throws IOException {
    var responseNotNulls = executeQuery(String.format("SELECT "
        + " max(int0) over (PARTITION BY `datetime1`),"
        + " min(int0) over (PARTITION BY `datetime1`),"
        + " avg(int0) over (PARTITION BY `datetime1`)"
        + "from %s where int0 IS NOT NULL;", TEST_INDEX_CALCS));
    var responseAllValues = executeQuery(String.format("SELECT "
        + " max(int0) over (PARTITION BY `datetime1`),"
        + " min(int0) over (PARTITION BY `datetime1`),"
        + " avg(int0) over (PARTITION BY `datetime1`)"
        + "from %s;", TEST_INDEX_CALCS));
    verifySchema(responseNotNulls,
        schema("max(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("min(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("avg(int0) over (PARTITION BY `datetime1`)", null, "double"));
    verifySchema(responseAllValues,
        schema("max(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("min(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("avg(int0) over (PARTITION BY `datetime1`)", null, "double"));
    assertEquals(responseNotNulls.query("/datarows/0/0"), responseAllValues.query("/datarows/0/0"));
    assertEquals(responseNotNulls.query("/datarows/0/1"), responseAllValues.query("/datarows/0/1"));
    assertEquals(responseNotNulls.query("/datarows/0/2"), responseAllValues.query("/datarows/0/2"));
  }

  @Test
  public void testPushDownAggregationOnNullNumericValuesReturnsNull() throws IOException {
    var response = executeQuery(String.format("SELECT "
        + "max(int0), min(int0), avg(int0) from %s where int0 IS NULL;", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(int0)", null, "integer"),
        schema("min(int0)", null, "integer"),
        schema("avg(int0)", null, "double"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnNullDateTimeValuesFromTableReturnsNull() throws IOException {
    var response = executeQuery(String.format("SELECT "
        + "max(datetime1), min(datetime1), avg(datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(datetime1)", null, "timestamp"),
        schema("min(datetime1)", null, "timestamp"),
        schema("avg(datetime1)", null, "timestamp"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnNullDateValuesReturnsNull() throws IOException {
    var response = executeQuery(String.format("SELECT "
        + "max(CAST(NULL AS date)), min(CAST(NULL AS date)), avg(CAST(NULL AS date)) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(CAST(NULL AS date))", null, "date"),
        schema("min(CAST(NULL AS date))", null, "date"),
        schema("avg(CAST(NULL AS date))", null, "date"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnNullTimeValuesReturnsNull() throws IOException {
    var response = executeQuery(String.format("SELECT "
        + "max(CAST(NULL AS time)), min(CAST(NULL AS time)), avg(CAST(NULL AS time)) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(CAST(NULL AS time))", null, "time"),
        schema("min(CAST(NULL AS time))", null, "time"),
        schema("avg(CAST(NULL AS time))", null, "time"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnNullTimeStampValuesReturnsNull() throws IOException {
    var response = executeQuery(String.format("SELECT "
        + "max(CAST(NULL AS timestamp)), min(CAST(NULL AS timestamp)), avg(CAST(NULL AS timestamp)) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(CAST(NULL AS timestamp))", null, "timestamp"),
        schema("min(CAST(NULL AS timestamp))", null, "timestamp"),
        schema("avg(CAST(NULL AS timestamp))", null, "timestamp"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnNullDateTimeValuesReturnsNull() throws IOException {
    var response = executeQuery(String.format("SELECT "
        + "max(datetime(NULL)), min(datetime(NULL)), avg(datetime(NULL)) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(datetime(NULL))", null, "datetime"),
        schema("min(datetime(NULL))", null, "datetime"),
        schema("avg(datetime(NULL))", null, "datetime"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnAllValuesAndOnNotNullReturnsSameResult() throws IOException {
    var responseNotNulls = executeQuery(String.format("SELECT "
        + "max(int0), min(int0), avg(int0) from %s where int0 IS NOT NULL;", TEST_INDEX_CALCS));
    var responseAllValues = executeQuery(String.format("SELECT "
        + "max(int0), min(int0), avg(int0) from %s;", TEST_INDEX_CALCS));
    verifySchema(responseNotNulls,
        schema("max(int0)", null, "integer"),
        schema("min(int0)", null, "integer"),
        schema("avg(int0)", null, "double"));
    verifySchema(responseAllValues,
        schema("max(int0)", null, "integer"),
        schema("min(int0)", null, "integer"),
        schema("avg(int0)", null, "double"));
    assertEquals(responseNotNulls.query("/datarows/0/0"), responseAllValues.query("/datarows/0/0"));
    assertEquals(responseNotNulls.query("/datarows/0/1"), responseAllValues.query("/datarows/0/1"));
    assertEquals(responseNotNulls.query("/datarows/0/2"), responseAllValues.query("/datarows/0/2"));
  }

  @Test
  public void testPushDownAndInMemoryAggregationReturnTheSameResult() throws IOException {
    // Playing with 'over (PARTITION BY `datetime1`)' - `datetime1` column has the same value for all rows
    // so partitioning by this column has no sense and doesn't (shouldn't) affect the results
    // Aggregations with `OVER` clause are executed in memory (in SQL plugin memory),
    // Aggregations without it are performed the OpenSearch node itself (pushed down to opensearch)
    // Going to compare results of `min`, `max` and `avg` aggregation on all numeric columns in `calcs`
    var columns = List.of("int0", "int1", "int2", "int3", "num0", "num1", "num2", "num3", "num4");
    var aggregations = List.of("min", "max", "avg");
    var inMemoryAggregQuery = new StringBuilder("SELECT ");
    var pushDownAggregQuery = new StringBuilder("SELECT ");
    for (var col : columns) {
      for (var aggreg : aggregations) {
        inMemoryAggregQuery.append(String.format(" %s(%s) over (PARTITION BY `datetime1`),", aggreg, col));
        pushDownAggregQuery.append(String.format(" %s(%s),", aggreg, col));
      }
    }
    // delete last comma
    inMemoryAggregQuery.deleteCharAt(inMemoryAggregQuery.length() - 1);
    pushDownAggregQuery.deleteCharAt(pushDownAggregQuery.length() - 1);

    var responseInMemory = executeQuery(
        inMemoryAggregQuery.append("from " + TEST_INDEX_CALCS).toString());
    var responsePushDown = executeQuery(
        pushDownAggregQuery.append("from " + TEST_INDEX_CALCS).toString());

    for (int i = 0; i < columns.size() * aggregations.size(); i++) {
      assertEquals(
          ((Number)responseInMemory.query("/datarows/0/" + i)).doubleValue(),
          ((Number)responsePushDown.query("/datarows/0/" + i)).doubleValue(),
          0.0000001); // a minor delta is affordable
    }
  }

  public void testMinIntegerPushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT min(int2)"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(int2)", null, "integer"));
    verifyDataRows(response, rows(-9));
  }

  @Test
  public void testMaxIntegerPushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT max(int2)"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(int2)", null, "integer"));
    verifyDataRows(response, rows(9));
  }

  @Test
  public void testAvgIntegerPushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT avg(int2)"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(int2)", null, "double"));
    verifyDataRows(response, rows(-0.8235294117647058D));
  }

  @Test
  public void testMinDoublePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT min(num3)"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(num3)", null, "double"));
    verifyDataRows(response, rows(-19.96D));
  }

  @Test
  public void testMaxDoublePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT max(num3)"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(num3)", null, "double"));
    verifyDataRows(response, rows(12.93D));
  }

  @Test
  public void testAvgDoublePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT avg(num3)"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(num3)", null, "double"));
    verifyDataRows(response, rows(-6.12D));
  }

  @Test
  public void testMinIntegerInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT min(int2)"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("min(int2) OVER(PARTITION BY datetime1)", null, "integer"));
    verifySome(response.getJSONArray("datarows"), rows(-9));
  }

  @Test
  public void testMaxIntegerInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT max(int2)"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(int2) OVER(PARTITION BY datetime1)", null, "integer"));
    verifySome(response.getJSONArray("datarows"), rows(9));
  }

  @Test
  public void testAvgIntegerInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT avg(int2)"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("avg(int2) OVER(PARTITION BY datetime1)", null, "double"));
    verifySome(response.getJSONArray("datarows"), rows(-0.8235294117647058D));
  }

  @Test
  public void testMinDoubleInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT min(num3)"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("min(num3) OVER(PARTITION BY datetime1)", null, "double"));
    verifySome(response.getJSONArray("datarows"), rows(-19.96D));
  }

  @Test
  public void testMaxDoubleInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT max(num3)"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(num3) OVER(PARTITION BY datetime1)", null, "double"));
    verifySome(response.getJSONArray("datarows"), rows(12.93D));
  }

  @Test
  public void testAvgDoubleInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT avg(num3)"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("avg(num3) OVER(PARTITION BY datetime1)", null, "double"));
    verifySome(response.getJSONArray("datarows"), rows(-6.12D));
  }

  @Test
  public void testMaxDatePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT max(CAST(date0 AS date))"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(CAST(date0 AS date))", null, "date"));
    verifyDataRows(response, rows("2004-06-19"));
  }

  @Test
  public void testAvgDatePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT avg(CAST(date0 AS date))"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(CAST(date0 AS date))", null, "date"));
    verifyDataRows(response, rows("1992-04-23"));
  }

  @Test
  public void testMinDateTimePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT min(datetime(CAST(time0 AS STRING)))"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(datetime(CAST(time0 AS STRING)))", null, "datetime"));
    verifyDataRows(response, rows("1899-12-30 21:07:32"));
  }

  @Test
  public void testMaxDateTimePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT max(datetime(CAST(time0 AS STRING)))"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(datetime(CAST(time0 AS STRING)))", null, "datetime"));
    verifyDataRows(response, rows("1900-01-01 20:36:00"));
  }

  @Test
  public void testAvgDateTimePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT avg(datetime(CAST(time0 AS STRING)))"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(datetime(CAST(time0 AS STRING)))", null, "datetime"));
    verifyDataRows(response, rows("1900-01-01 03:35:00.236"));
  }

  @Test
  public void testMinTimePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT min(CAST(time1 AS time))"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(CAST(time1 AS time))", null, "time"));
    verifyDataRows(response, rows("00:05:57"));
  }

  @Test
  public void testMaxTimePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT max(CAST(time1 AS time))"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(CAST(time1 AS time))", null, "time"));
    verifyDataRows(response, rows("22:50:16"));
  }

  @Test
  public void testAvgTimePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT avg(CAST(time1 AS time))"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(CAST(time1 AS time))", null, "time"));
    verifyDataRows(response, rows("13:06:36.25"));
  }

  @Test
  public void testMinTimeStampPushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT min(CAST(datetime0 AS timestamp))"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(CAST(datetime0 AS timestamp))", null, "timestamp"));
    verifyDataRows(response, rows("2004-07-04 22:49:28"));
  }

  @Test
  public void testMaxTimeStampPushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT max(CAST(datetime0 AS timestamp))"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(CAST(datetime0 AS timestamp))", null, "timestamp"));
    verifyDataRows(response, rows("2004-08-02 07:59:23"));
  }

  @Test
  public void testAvgTimeStampPushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT avg(CAST(datetime0 AS timestamp))"
        + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(CAST(datetime0 AS timestamp))", null, "timestamp"));
    verifyDataRows(response, rows("2004-07-20 10:38:09.705"));
  }

  @Test
  public void testMinDateInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT min(CAST(date0 AS date))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("min(CAST(date0 AS date)) OVER(PARTITION BY datetime1)", null, "date"));
    verifySome(response.getJSONArray("datarows"), rows("1972-07-04"));
  }

  @Test
  public void testMaxDateInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT max(CAST(date0 AS date))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(CAST(date0 AS date)) OVER(PARTITION BY datetime1)", null, "date"));
    verifySome(response.getJSONArray("datarows"), rows("2004-06-19"));
  }

  @Test
  public void testAvgDateInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT avg(CAST(date0 AS date))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("avg(CAST(date0 AS date)) OVER(PARTITION BY datetime1)", null, "date"));
    verifySome(response.getJSONArray("datarows"), rows("1992-04-23"));
  }

  @Test
  public void testMinDateTimeInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT min(datetime(CAST(time0 AS STRING)))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("min(datetime(CAST(time0 AS STRING))) OVER(PARTITION BY datetime1)", null, "datetime"));
    verifySome(response.getJSONArray("datarows"), rows("1899-12-30 21:07:32"));
  }

  @Test
  public void testMaxDateTimeInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT max(datetime(CAST(time0 AS STRING)))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(datetime(CAST(time0 AS STRING))) OVER(PARTITION BY datetime1)", null, "datetime"));
    verifySome(response.getJSONArray("datarows"), rows("1900-01-01 20:36:00"));
  }

  @Test
  public void testAvgDateTimeInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT avg(datetime(CAST(time0 AS STRING)))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("avg(datetime(CAST(time0 AS STRING))) OVER(PARTITION BY datetime1)", null, "datetime"));
    verifySome(response.getJSONArray("datarows"), rows("1900-01-01 03:35:00.236"));
  }

  @Test
  public void testMinTimeInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT min(CAST(time1 AS time))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("min(CAST(time1 AS time)) OVER(PARTITION BY datetime1)", null, "time"));
    verifySome(response.getJSONArray("datarows"), rows("00:05:57"));
  }

  @Test
  public void testMaxTimeInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT max(CAST(time1 AS time))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(CAST(time1 AS time)) OVER(PARTITION BY datetime1)", null, "time"));
    verifySome(response.getJSONArray("datarows"), rows("22:50:16"));
  }

  @Test
  public void testAvgTimeInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT avg(CAST(time1 AS time))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("avg(CAST(time1 AS time)) OVER(PARTITION BY datetime1)", null, "time"));
    verifySome(response.getJSONArray("datarows"), rows("13:06:36.25"));
  }

  @Test
  public void testMinTimeStampInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT min(CAST(datetime0 AS timestamp))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("min(CAST(datetime0 AS timestamp)) OVER(PARTITION BY datetime1)", null, "timestamp"));
    verifySome(response.getJSONArray("datarows"), rows("2004-07-04 22:49:28"));
  }

  @Test
  public void testMaxTimeStampInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT max(CAST(datetime0 AS timestamp))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(CAST(datetime0 AS timestamp)) OVER(PARTITION BY datetime1)", null, "timestamp"));
    verifySome(response.getJSONArray("datarows"), rows("2004-08-02 07:59:23"));
  }

  @Test
  public void testAvgTimeStampInMemory() throws IOException {
    var response = executeQuery(String.format("SELECT avg(CAST(datetime0 AS timestamp))"
        + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("avg(CAST(datetime0 AS timestamp)) OVER(PARTITION BY datetime1)", null, "timestamp"));
    verifySome(response.getJSONArray("datarows"), rows("2004-07-20 10:38:09.705"));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void countTest() throws IOException {

    JSONObject result = executeQuery(String.format("SELECT COUNT(*) FROM %s", TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    Assert.assertThat(getIntAggregationValue(result, "COUNT(*)", "value"), equalTo(1000));
  }

  @org.junit.Test
  public void countDistinctTest() {
    JSONObject response = executeJdbcRequest(
        String.format("SELECT COUNT(distinct gender) FROM %s", TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("COUNT(distinct gender)", null, "integer"));
    verifyDataRows(response, rows(2));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void countWithDocsHintTest() throws Exception {

    JSONObject result =
        executeQuery(String.format("SELECT /*! DOCS_WITH_AGGREGATION(10) */ count(*) from %s",
            TEST_INDEX_ACCOUNT));
    JSONArray hits = (JSONArray) result.query("/hits/hits");
    Assert.assertThat(hits.length(), equalTo(10));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void sumTest() throws IOException {

    JSONObject result =
        executeQuery(String.format("SELECT SUM(balance) FROM %s", TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    Assert.assertThat(getDoubleAggregationValue(result, "SUM(balance)", "value"),
        equalTo(25714837.0));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void minTest() throws IOException {

    JSONObject result = executeQuery(String.format("SELECT MIN(age) FROM %s", TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    Assert.assertThat(getDoubleAggregationValue(result, "MIN(age)", "value"), equalTo(20.0));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void maxTest() throws IOException {

    JSONObject result = executeQuery(String.format("SELECT MAX(age) FROM %s", TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    Assert.assertThat(getDoubleAggregationValue(result, "MAX(age)", "value"), equalTo(40.0));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void avgTest() throws IOException {

    JSONObject result = executeQuery(String.format("SELECT AVG(age) FROM %s", TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    Assert.assertThat(getDoubleAggregationValue(result, "AVG(age)", "value"), equalTo(30.171));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void statsTest() throws IOException {

    JSONObject result =
        executeQuery(String.format("SELECT STATS(age) FROM %s", TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    Assert.assertThat(getIntAggregationValue(result, "STATS(age)", "count"), equalTo(1000));
    Assert.assertThat(getDoubleAggregationValue(result, "STATS(age)", "min"), equalTo(20.0));
    Assert.assertThat(getDoubleAggregationValue(result, "STATS(age)", "max"), equalTo(40.0));
    Assert.assertThat(getDoubleAggregationValue(result, "STATS(age)", "avg"), equalTo(30.171));
    Assert.assertThat(getDoubleAggregationValue(result, "STATS(age)", "sum"), equalTo(30171.0));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void extendedStatsTest() throws IOException {

    JSONObject result = executeQuery(String.format("SELECT EXTENDED_STATS(age) FROM %s",
        TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    Assert
        .assertThat(getDoubleAggregationValue(result, "EXTENDED_STATS(age)", "min"), equalTo(20.0));
    Assert
        .assertThat(getDoubleAggregationValue(result, "EXTENDED_STATS(age)", "max"), equalTo(40.0));
    Assert.assertThat(getDoubleAggregationValue(result, "EXTENDED_STATS(age)", "avg"),
        equalTo(30.171));
    Assert.assertThat(getDoubleAggregationValue(result, "EXTENDED_STATS(age)", "sum"),
        equalTo(30171.0));
    Assert.assertThat(getDoubleAggregationValue(result, "EXTENDED_STATS(age)", "sum_of_squares"),
        equalTo(946393.0));
    Assert.assertEquals(6.008640362012022,
        getDoubleAggregationValue(result, "EXTENDED_STATS(age)", "std_deviation"), 0.0001);
    Assert.assertEquals(36.10375899999996,
        getDoubleAggregationValue(result, "EXTENDED_STATS(age)", "variance"), 0.0001);
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void percentileTest() throws IOException {

    JSONObject result =
        executeQuery(String.format("SELECT PERCENTILES(age) FROM %s", TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    Assert
        .assertEquals(20.0, getDoubleAggregationValue(result, "PERCENTILES(age)", "values", "1.0"),
            0.001);
    Assert
        .assertEquals(21.0, getDoubleAggregationValue(result, "PERCENTILES(age)", "values", "5.0"),
            0.001);
    Assert
        .assertEquals(25.0, getDoubleAggregationValue(result, "PERCENTILES(age)", "values", "25.0"),
            0.001);
    // All percentiles are approximations calculated by t-digest, however, P50 has the widest distribution (not sure why)
    Assert
        .assertEquals(30.5, getDoubleAggregationValue(result, "PERCENTILES(age)", "values", "50.0"),
            0.6);
    Assert
        .assertEquals(35.0, getDoubleAggregationValue(result, "PERCENTILES(age)", "values", "75.0"),
            0.6);
    Assert
        .assertEquals(39.0, getDoubleAggregationValue(result, "PERCENTILES(age)", "values", "95.0"),
            0.6);
    Assert
        .assertEquals(40.0, getDoubleAggregationValue(result, "PERCENTILES(age)", "values", "99.0"),
            0.6);
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void percentileTestSpecific() throws IOException {

    JSONObject result = executeQuery(String.format("SELECT PERCENTILES(age,25.0,75.0) FROM %s",
        TEST_INDEX_ACCOUNT));

    Assert.assertThat(getTotalHits(result), equalTo(1000));
    Assert.assertEquals(25.0,
        getDoubleAggregationValue(result, "PERCENTILES(age,25.0,75.0)", "values", "25.0"), 0.6);
    Assert.assertEquals(35.0,
        getDoubleAggregationValue(result, "PERCENTILES(age,25.0,75.0)", "values", "75.0"), 0.6);
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void aliasTest() throws IOException {

    JSONObject result = executeQuery(String.format("SELECT COUNT(*) AS mycount FROM %s",
        TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    Assert.assertThat(getIntAggregationValue(result, "mycount", "value"), equalTo(1000));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void groupByTest() throws Exception {
    JSONObject result = executeQuery(String.format("SELECT COUNT(*) FROM %s GROUP BY gender",
        TEST_INDEX_ACCOUNT));
    assertResultForGroupByTest(result);
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void groupByUsingTableAliasTest() throws Exception {
    JSONObject result = executeQuery(String.format("SELECT COUNT(*) FROM %s a GROUP BY a.gender",
        TEST_INDEX_ACCOUNT));
    assertResultForGroupByTest(result);
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void groupByUsingTableNamePrefixTest() throws Exception {
    JSONObject result = executeQuery(String.format(
        "SELECT COUNT(*) FROM %s GROUP BY opensearch-sql_test_index_account.gender",
        TEST_INDEX_ACCOUNT
    ));
    assertResultForGroupByTest(result);
  }

  private void assertResultForGroupByTest(JSONObject result) {
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    JSONObject gender = getAggregation(result, "gender");
    Assert.assertThat(gender.getJSONArray("buckets").length(), equalTo(2));

    final boolean isMaleFirst = gender.optQuery("/buckets/0/key").equals("m");
    final int maleBucketId = isMaleFirst ? 0 : 1;
    final int femaleBucketId = isMaleFirst ? 1 : 0;

    final String maleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", maleBucketId);
    final String femaleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", femaleBucketId);

    Assert.assertThat(gender.query(maleBucketPrefix + "/key"), equalTo("m"));
    Assert.assertThat(gender.query(maleBucketPrefix + "/COUNT(*)/value"), equalTo(507));
    Assert.assertThat(gender.query(femaleBucketPrefix + "/key"), equalTo("f"));
    Assert.assertThat(gender.query(femaleBucketPrefix + "/COUNT(*)/value"), equalTo(493));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void groupByHavingTest() throws Exception {
    JSONObject result = executeQuery(String.format(
        "SELECT gender " +
            "FROM %s " +
            "GROUP BY gender " +
            "HAVING COUNT(*) > 0", TEST_INDEX_ACCOUNT));
    assertResultForGroupByHavingTest(result);
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void groupByHavingUsingTableAliasTest() throws Exception {
    JSONObject result = executeQuery(String.format(
        "SELECT a.gender " +
            "FROM %s a " +
            "GROUP BY a.gender " +
            "HAVING COUNT(*) > 0", TEST_INDEX_ACCOUNT));
    assertResultForGroupByHavingTest(result);
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void groupByHavingUsingTableNamePrefix throws Exception {
    JSONObject result = executeQuery(String.format(
        "SELECT opensearch-sql_test_index_account.gender " +
            "FROM %s " +
            "GROUP BY opensearch-sql_test_index_account.gender " +
            "HAVING COUNT(*) > 0", TEST_INDEX_ACCOUNT));
    assertResultForGroupByHavingTest(result);
  }

  private void assertResultForGroupByHavingTest(JSONObject result) {
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    JSONObject gender = getAggregation(result, "gender");
    Assert.assertThat(gender.getJSONArray("buckets").length(), equalTo(2));

    final boolean isMaleFirst = gender.optQuery("/buckets/0/key").equals("m");
    final int maleBucketId = isMaleFirst ? 0 : 1;
    final int femaleBucketId = isMaleFirst ? 1 : 0;

    final String maleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", maleBucketId);
    final String femaleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", femaleBucketId);

    Assert.assertThat(gender.query(maleBucketPrefix + "/key"), equalTo("m"));
    Assert.assertThat(gender.query(maleBucketPrefix + "/count_0/value"), equalTo(507));
    Assert.assertThat(gender.query(femaleBucketPrefix + "/key"), equalTo("f"));
    Assert.assertThat(gender.query(femaleBucketPrefix + "/count_0/value"), equalTo(493));
  }

  @Ignore //todo VerificationException: table alias or field name missing
  @org.junit.Test
  public void groupBySubqueryTest() throws Exception {

    JSONObject result = executeQuery(String.format(
        "SELECT COUNT(*) FROM %s " +
            "WHERE firstname IN (SELECT firstname FROM %s) " +
            "GROUP BY gender",
        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    JSONObject gender = getAggregation(result, "gender");
    Assert.assertThat(gender.getJSONArray("buckets").length(), equalTo(2));

    final boolean isMaleFirst = gender.optQuery("/buckets/0/key").equals("m");
    final int maleBucketId = isMaleFirst ? 0 : 1;
    final int femaleBucketId = isMaleFirst ? 1 : 0;

    final String maleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", maleBucketId);
    final String femaleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", femaleBucketId);

    Assert.assertThat(gender.query(maleBucketPrefix + "/key"), equalTo("m"));
    Assert.assertThat(gender.query(maleBucketPrefix + "/COUNT(*)/value"), equalTo(507));
    Assert.assertThat(gender.query(femaleBucketPrefix + "/key"), equalTo("f"));
    Assert.assertThat(gender.query(femaleBucketPrefix + "/COUNT(*)/value"), equalTo(493));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void postFilterTest() throws Exception {

    JSONObject result = executeQuery(String.format("SELECT /*! POST_FILTER({\\\"term\\\":" +
            "{\\\"gender\\\":\\\"m\\\"}}) */ COUNT(*) FROM %s GROUP BY gender",
        TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(507));
    JSONObject gender = getAggregation(result, "gender");
    Assert.assertThat(gender.getJSONArray("buckets").length(), equalTo(2));

    final boolean isMaleFirst = gender.optQuery("/buckets/0/key").equals("m");
    final int maleBucketId = isMaleFirst ? 0 : 1;
    final int femaleBucketId = isMaleFirst ? 1 : 0;

    final String maleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", maleBucketId);
    final String femaleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", femaleBucketId);

    Assert.assertThat(gender.query(maleBucketPrefix + "/key"), equalTo("m"));
    Assert.assertThat(gender.query(maleBucketPrefix + "/COUNT(*)/value"), equalTo(507));
    Assert.assertThat(gender.query(femaleBucketPrefix + "/key"), equalTo("f"));
    Assert.assertThat(gender.query(femaleBucketPrefix + "/COUNT(*)/value"), equalTo(493));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void multipleGroupByTest() throws Exception {

    JSONObject result = executeQuery(String.format("SELECT COUNT(*) FROM %s GROUP BY gender," +
            " terms('field'='age','size'=200,'alias'='age')",
        TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    JSONObject gender = getAggregation(result, "gender");
    Assert.assertThat(gender.getJSONArray("buckets").length(), equalTo(2));

    final boolean isMaleFirst = gender.optQuery("/buckets/0/key").equals("m");
    final int maleBucketId = isMaleFirst ? 0 : 1;
    final int femaleBucketId = isMaleFirst ? 1 : 0;

    final String maleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", maleBucketId);
    final String femaleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", femaleBucketId);

    final JSONArray mAgeBuckets = (JSONArray) (gender.optQuery(maleBucketPrefix + "/age/buckets"));
    final JSONArray fAgeBuckets =
        (JSONArray) (gender.optQuery(femaleBucketPrefix + "/age/buckets"));

    final Set<Integer> expectedAges =
        IntStream.range(20, 41).boxed().collect(Collectors.toCollection(HashSet::new));
    Assert.assertThat(mAgeBuckets.length(), equalTo(expectedAges.size()));
    Assert.assertThat(fAgeBuckets.length(), equalTo(expectedAges.size()));

    final Set<Integer> actualAgesM = new HashSet<>(expectedAges.size());
    final Set<Integer> actualAgesF = new HashSet<>(expectedAges.size());
    mAgeBuckets.iterator()
        .forEachRemaining(json -> actualAgesM.add(((JSONObject) json).getInt("key")));
    fAgeBuckets.iterator()
        .forEachRemaining(json -> actualAgesF.add(((JSONObject) json).getInt("key")));

    Assert.assertThat(actualAgesM, equalTo(expectedAges));
    Assert.assertThat(actualAgesF, equalTo(expectedAges));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void multipleGroupBysWithSize() throws Exception {

    JSONObject result = executeQuery(String.format("SELECT COUNT(*) FROM %s GROUP BY gender," +
            " terms('alias'='ageAgg','field'='age','size'=3)",
        TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    JSONObject gender = getAggregation(result, "gender");
    Assert.assertThat(gender.getJSONArray("buckets").length(), equalTo(2));

    final JSONArray mAgeBuckets = (JSONArray) (gender.optQuery("/buckets/0/ageAgg/buckets"));
    final JSONArray fAgeBuckets = (JSONArray) (gender.optQuery("/buckets/0/ageAgg/buckets"));

    Assert.assertThat(mAgeBuckets.length(), equalTo(3));
    Assert.assertThat(fAgeBuckets.length(), equalTo(3));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void termsWithSize() throws Exception {

    JSONObject result = executeQuery(String.format("SELECT COUNT(*) FROM %s GROUP BY terms" +
            "('alias'='ageAgg','field'='age','size'=3)",
        TEST_INDEX_ACCOUNT));
    Assert.assertThat(getTotalHits(result), equalTo(1000));
    JSONObject gender = getAggregation(result, "ageAgg");
    Assert.assertThat(gender.getJSONArray("buckets").length(), equalTo(3));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void termsWithMissing() throws Exception {

    JSONObject result = executeQuery(String.format("SELECT count(*) FROM %s GROUP BY terms" +
            "('alias'='nick','field'='nickname','missing'='no_nickname')",
        TEST_INDEX_GAME_OF_THRONES));
    JSONObject nick = getAggregation(result, "nick");

    Optional<JSONObject> noNicknameBucket = Optional.empty();
    Iterator<Object> iter = nick.getJSONArray("buckets").iterator();
    while (iter.hasNext()) {
      JSONObject bucket = (JSONObject) iter.next();
      if (bucket.getString("key").equals("no_nickname")) {
        noNicknameBucket = Optional.of(bucket);
        Assert.assertThat(bucket.getInt("doc_count"), equalTo(6));
      }
    }
    Assert.assertTrue(noNicknameBucket.isPresent());
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void termsWithOrder() throws Exception {

    final String dog1 = "snoopy";
    final String dog2 = "rex";

    JSONObject result = executeQuery(String.format("SELECT count(*) FROM %s GROUP BY terms" +
            "('field'='dog_name', 'alias'='dog_name', 'order'='desc')",
        TEST_INDEX_DOG));
    JSONObject dogName = getAggregation(result, "dog_name");

    String firstDog = (String) (dogName.optQuery("/buckets/0/key"));
    String secondDog = (String) (dogName.optQuery("/buckets/1/key"));
    Assert.assertThat(firstDog, equalTo(dog1));
    Assert.assertThat(secondDog, equalTo(dog2));

    result = executeQuery(String.format("SELECT count(*) FROM %s GROUP BY terms" +
        "('field'='dog_name', 'alias'='dog_name', 'order'='asc')", TEST_INDEX_DOG));

    dogName = getAggregation(result, "dog_name");

    firstDog = (String) (dogName.optQuery("/buckets/0/key"));
    secondDog = (String) (dogName.optQuery("/buckets/1/key"));
    Assert.assertThat(firstDog, equalTo(dog2));
    Assert.assertThat(secondDog, equalTo(dog1));
  }

  @org.junit.Test
  public void orderByAscTest() {
    JSONObject response = executeJdbcRequest(String.format("SELECT COUNT(*) FROM %s " +
        "GROUP BY gender ORDER BY COUNT(*)", TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("COUNT(*)", null, "integer"));
    verifyDataRows(response,
        rows(493),
        rows(507));
  }

  @org.junit.Test
  public void orderByAliasAscTest() {
    JSONObject response = executeJdbcRequest(String.format("SELECT COUNT(*) as count FROM %s " +
        "GROUP BY gender ORDER BY count", TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("COUNT(*)", "count", "integer"));
    verifyDataRowsInOrder(response,
        rows(493),
        rows(507));
  }

  @org.junit.Test
  public void orderByDescTest() throws IOException {
    JSONObject response = executeJdbcRequest(String.format("SELECT COUNT(*) FROM %s " +
        "GROUP BY gender ORDER BY COUNT(*) DESC", TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("COUNT(*)", null, "integer"));
    verifyDataRowsInOrder(response,
        rows(507),
        rows(493));
  }

  @org.junit.Test
  public void orderByAliasDescTest() throws IOException {
    JSONObject response = executeJdbcRequest(String.format("SELECT COUNT(*) as count FROM %s " +
        "GROUP BY gender ORDER BY count DESC", TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("COUNT(*)", "count", "integer"));
    verifyDataRowsInOrder(response,
        rows(507),
        rows(493));
  }

  @org.junit.Test
  public void orderByGroupFieldWithAlias() throws IOException {
    // ORDER BY field name
    JSONObject response = executeJdbcRequest(String.format("SELECT gender as g, COUNT(*) as count "
        + "FROM %s GROUP BY gender ORDER BY gender", TEST_INDEX_ACCOUNT));

    verifySchema(response,
        schema("gender", "g", "text"),
        schema("COUNT(*)", "count", "integer"));
    verifyDataRowsInOrder(response,
        rows("f", 493),
        rows("m", 507));

    // ORDER BY field alias
    response = executeJdbcRequest(String.format("SELECT gender as g, COUNT(*) as count "
        + "FROM %s GROUP BY gender ORDER BY g", TEST_INDEX_ACCOUNT));

    verifySchema(response,
        schema("gender", "g", "text"),
        schema("COUNT(*)", "count", "integer"));
    verifyDataRowsInOrder(response,
        rows("f", 493),
        rows("m", 507));
  }

  @org.junit.Test
  public void limitTest() throws IOException {
    JSONObject response = executeJdbcRequest(String.format("SELECT COUNT(*) FROM %s " +
        "GROUP BY age ORDER BY COUNT(*) LIMIT 5", TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("COUNT(*)", null, "integer"));
    verifyDataRowsInOrder(response,
        rows(35),
        rows(39),
        rows(39),
        rows(42),
        rows(42));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void countGroupByRange() throws IOException {

    JSONObject result = executeQuery(String.format("SELECT COUNT(age) FROM %s" +
        " GROUP BY range(age, 20,25,30,35,40)", TEST_INDEX_ACCOUNT));
    JSONObject ageAgg = getAggregation(result, "range(age,20,25,30,35,40)");
    JSONArray buckets = ageAgg.getJSONArray("buckets");
    Assert.assertThat(buckets.length(), equalTo(4));

    final int[] expectedResults = new int[] {225, 226, 259, 245};

    for (int i = 0; i < expectedResults.length; ++i) {

      Assert.assertThat(buckets.query(String.format(Locale.ROOT, "/%d/COUNT(age)/value", i)),
          equalTo(expectedResults[i]));
    }
  }

  /**
   * <a>http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html</a>
   */
  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void countGroupByDateTest() throws IOException {

    String result =
        explainQuery(String.format("select insert_time from %s group by date_histogram" +
                "('field'='insert_time','fixed_interval'='1h','format'='yyyy-MM','min_doc_count'=5) ",
            TEST_INDEX_ONLINE));
    Assert.assertThat(result.replaceAll("\\s+", ""),
        containsString("{\"date_histogram\":{\"field\":\"insert_time\",\"format\":\"yyyy-MM\"," +
            "\"fixed_interval\":\"1h\",\"offset\":0,\"order\":{\"_key\":\"asc\"},\"keyed\":false," +
            "\"min_doc_count\":5}"));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void countGroupByDateTestWithAlias() throws IOException {
    String result =
        explainQuery(String.format("select insert_time from %s group by date_histogram" +
                "('field'='insert_time','fixed_interval'='1h','format'='yyyy-MM','alias'='myAlias')",
            TEST_INDEX_ONLINE));
    Assert.assertThat(result.replaceAll("\\s+", ""),
        containsString("myAlias\":{\"date_histogram\":{\"field\":\"insert_time\"," +
            "\"format\":\"yyyy-MM\",\"fixed_interval\":\"1h\""));
  }

//    /**
//     * <a>http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html</a>
//     */
//    @Test
//    public void countDateRangeTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
//        String result = explainQuery(String.format("select online from %s group by date_range(field='insert_time'," +
//                "'format'='yyyy-MM-dd' ,'2014-08-18','2014-08-17','now-8d','now-7d','now-6d','now')",
//                TEST_INDEX_ONLINE));
//        // TODO: fix the query or fix the code for the query to work
//    }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void topHitTest() throws IOException {

    String query = String
        .format("select topHits('size'=3,age='desc') from %s group by gender", TEST_INDEX_ACCOUNT);
    JSONObject result = executeQuery(query);
    JSONObject gender = getAggregation(result, "gender");
    Assert.assertThat(gender.getJSONArray("buckets").length(), equalTo(2));

    final boolean isMaleFirst = gender.optQuery("/buckets/0/key").equals("m");
    final int maleBucketId = isMaleFirst ? 0 : 1;
    final int femaleBucketId = isMaleFirst ? 1 : 0;

    final String maleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", maleBucketId);
    final String femaleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", femaleBucketId);

    Assert.assertThat(gender.query(maleBucketPrefix + "/key"), equalTo("m"));
    Assert.assertThat(gender.query(maleBucketPrefix + "/topHits(size=3,age=desc)/hits/total/value"),
        equalTo(507));
    Assert.assertThat(
        gender.query(maleBucketPrefix + "/topHits(size=3,age=desc)/hits/total/relation"),
        equalTo("eq"));
    Assert.assertThat(
        ((JSONArray) gender.query(maleBucketPrefix + "/topHits(size=3,age=desc)/hits/hits"))
            .length(),
        equalTo(3));
    Assert.assertThat(gender.query(femaleBucketPrefix + "/key"), equalTo("f"));
    Assert
        .assertThat(gender.query(femaleBucketPrefix + "/topHits(size=3,age=desc)/hits/total/value"),
            equalTo(493));
    Assert.assertThat(
        gender.query(femaleBucketPrefix + "/topHits(size=3,age=desc)/hits/total/relation"),
        equalTo("eq"));
    Assert.assertThat(
        ((JSONArray) gender.query(femaleBucketPrefix + "/topHits(size=3,age=desc)/hits/hits"))
            .length(),
        equalTo(3));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void topHitTest_WithInclude() throws IOException {

    String query =
        String.format("select topHits('size'=3,age='desc','include'=age) from %s group by gender",
            TEST_INDEX_ACCOUNT);
    JSONObject result = executeQuery(query);
    JSONObject gender = getAggregation(result, "gender");
    Assert.assertThat(gender.getJSONArray("buckets").length(), equalTo(2));

    final boolean isMaleFirst = gender.optQuery("/buckets/0/key").equals("m");
    final int maleBucketId = isMaleFirst ? 0 : 1;
    final int femaleBucketId = isMaleFirst ? 1 : 0;

    final String maleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", maleBucketId);
    final String femaleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", femaleBucketId);

    Assert.assertThat(gender.query(maleBucketPrefix + "/key"), equalTo("m"));
    Assert.assertThat(
        gender.query(maleBucketPrefix + "/topHits(size=3,age=desc,include=age)/hits/total/value"),
        equalTo(507));
    Assert.assertThat(gender
            .query(maleBucketPrefix + "/topHits(size=3,age=desc,include=age)/hits/total/relation"),
        equalTo("eq"));
    Assert.assertThat(((JSONArray) gender.query(
            maleBucketPrefix + "/topHits(size=3,age=desc,include=age)/hits/hits")).length(),
        equalTo(3));

    Assert.assertThat(gender.query(femaleBucketPrefix + "/key"), equalTo("f"));
    Assert.assertThat(
        gender.query(femaleBucketPrefix + "/topHits(size=3,age=desc,include=age)/hits/total/value"),
        equalTo(493));
    Assert.assertThat(gender
            .query(femaleBucketPrefix + "/topHits(size=3,age=desc,include=age)/hits/total/relation"),
        equalTo("eq"));
    Assert.assertThat(((JSONArray) gender.query(
            femaleBucketPrefix + "/topHits(size=3,age=desc,include=age)/hits/hits")).length(),
        equalTo(3));

    for (int i = 0; i < 2; ++i) {
      for (int j = 0; j < 3; ++j) {
        JSONObject source = (JSONObject) gender.query(String.format(Locale.ROOT,
            "/buckets/%d/topHits(size=3,age=desc,include=age)/hits/hits/%d/_source", i, j));
        Assert.assertThat(source.length(), equalTo(1));
        Assert.assertTrue(source.has("age"));
        Assert.assertThat(source.getInt("age"), equalTo(40));
      }
    }
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void topHitTest_WithIncludeTwoFields() throws IOException {

    String query =
        String.format("select topHits('size'=3,'include'='age,firstname',age='desc') from %s " +
            "group by gender", TEST_INDEX_ACCOUNT);
    JSONObject result = executeQuery(query);
    JSONObject gender = getAggregation(result, "gender");
    Assert.assertThat(gender.getJSONArray("buckets").length(), equalTo(2));

    for (int i = 0; i < 2; ++i) {
      for (int j = 0; j < 3; ++j) {
        JSONObject source = (JSONObject) gender.query(String.format(Locale.ROOT,
            "/buckets/%d/topHits(size=3,include=age,firstname,age=desc)/hits/hits/%d/_source", i,
            j));
        Assert.assertThat(source.length(), equalTo(2));
        Assert.assertTrue(source.has("age"));
        Assert.assertThat(source.getInt("age"), equalTo(40));
        Assert.assertTrue(source.has("firstname"));
        final String name = source.getString("firstname");
        Assert.assertThat(name, not(isEmptyString()));
      }
    }
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void topHitTest_WithExclude() throws IOException {

    String query = String.format("select topHits('size'=3,'exclude'='lastname',age='desc') from " +
        "%s group by gender", TEST_INDEX_ACCOUNT);
    JSONObject result = executeQuery(query);
    JSONObject gender = getAggregation(result, "gender");
    Assert.assertThat(gender.getJSONArray("buckets").length(), equalTo(2));

    final boolean isMaleFirst = gender.optQuery("/buckets/0/key").equals("m");
    final int maleBucketId = isMaleFirst ? 0 : 1;
    final int femaleBucketId = isMaleFirst ? 1 : 0;

    final String maleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", maleBucketId);
    final String femaleBucketPrefix = String.format(Locale.ROOT, "/buckets/%d", femaleBucketId);

    Assert.assertThat(gender.query(maleBucketPrefix + "/key"), equalTo("m"));
    Assert.assertThat(gender
            .query(maleBucketPrefix + "/topHits(size=3,exclude=lastname,age=desc)/hits/total/value"),
        equalTo(507));
    Assert.assertThat(gender
            .query(maleBucketPrefix + "/topHits(size=3,exclude=lastname,age=desc)/hits/total/relation"),
        equalTo("eq"));
    Assert.assertThat(((JSONArray) gender.query(
            maleBucketPrefix + "/topHits(size=3,exclude=lastname,age=desc)/hits/hits")).length(),
        equalTo(3));

    Assert.assertThat(gender.query(femaleBucketPrefix + "/key"), equalTo("f"));
    Assert.assertThat(gender
            .query(femaleBucketPrefix + "/topHits(size=3,exclude=lastname,age=desc)/hits/total/value"),
        equalTo(493));
    Assert.assertThat(gender.query(
            femaleBucketPrefix + "/topHits(size=3,exclude=lastname,age=desc)/hits/total/relation"),
        equalTo("eq"));
    Assert.assertThat(((JSONArray) gender.query(
            femaleBucketPrefix + "/topHits(size=3,exclude=lastname,age=desc)/hits/hits")).length(),
        equalTo(3));

    final Set<String> expectedFields = new HashSet<>(Arrays.asList(
        "account_number",
        "firstname",
        "address",
        "balance",
        "gender",
        "city",
        "employer",
        "state",
        "age",
        "email"
    ));

    for (int i = 0; i < 2; ++i) {
      for (int j = 0; j < 3; ++j) {
        JSONObject source = (JSONObject) gender.query(String.format(Locale.ROOT,
            "/buckets/%d/topHits(size=3,exclude=lastname,age=desc)/hits/hits/%d/_source", i, j));
        Assert.assertThat(source.length(), equalTo(expectedFields.size()));
        Assert.assertFalse(source.has("lastname"));
        Assert.assertThat(source.keySet().containsAll(expectedFields), equalTo(true));
      }
    }
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void groupByOnNestedFieldWithFilterTest() throws Exception {

    String query = String.format("SELECT COUNT(*) FROM %s GROUP BY  nested(message.info)," +
        "filter('myFilter',message.info = 'a')", TEST_INDEX_NESTED_TYPE);
    JSONObject result = executeQuery(query);

    JSONObject aggregation = getAggregation(result, "message.info@NESTED");
    JSONArray buckets = (JSONArray) aggregation.optQuery("/myFilter@FILTER/message.info/buckets");
    Assert.assertNotNull(buckets);
    Assert.assertThat(buckets.length(), equalTo(1));

    JSONObject bucket = buckets.getJSONObject(0);
    Assert.assertThat(bucket.getString("key"), equalTo("a"));
    Assert.assertThat(bucket.query("/COUNT(*)/value"), equalTo(2));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void minOnNestedField() throws Exception {

    String query = String.format("SELECT min(nested(message.dayOfWeek)) as minDays FROM %s",
        TEST_INDEX_NESTED_TYPE);
    JSONObject result = executeQuery(query);
    JSONObject aggregation = getAggregation(result, "message.dayOfWeek@NESTED");
    Assert.assertEquals(1.0, (double) aggregation.query("/minDays/value"), 0.0001);
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void sumOnNestedField() throws Exception {

    String query = String.format("SELECT sum(nested(message.dayOfWeek)) as sumDays FROM %s",
        TEST_INDEX_NESTED_TYPE);
    JSONObject result = executeQuery(query);
    JSONObject aggregation = getAggregation(result, "message.dayOfWeek@NESTED");
    Assert.assertEquals(19.0, (double) aggregation.query("/sumDays/value"), 0.0001);
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void histogramOnNestedField() throws Exception {

    String query = String.format("select count(*) from %s group by histogram" +
            "('field'='message.dayOfWeek','nested'='message','interval'='2' , 'alias' = 'someAlias' )",
        TEST_INDEX_NESTED_TYPE);
    JSONObject result = executeQuery(query);
    JSONObject aggregation = getAggregation(result, "message@NESTED");

    final Map<Double, Integer> expectedCountsByKey = new HashMap<>();
    expectedCountsByKey.put(0.0, 2);
    expectedCountsByKey.put(2.0, 1);
    expectedCountsByKey.put(4.0, 2);
    expectedCountsByKey.put(6.0, 1);

    JSONArray buckets = (JSONArray) aggregation.query("/someAlias/buckets");
    Assert.assertThat(buckets.length(), equalTo(4));

    buckets.forEach(obj -> {
      JSONObject bucket = (JSONObject) obj;
      final double key = bucket.getDouble("key");
      Assert.assertTrue(expectedCountsByKey.containsKey(key));
      Assert.assertThat(bucket.getJSONObject("COUNT(*)").getInt("value"),
          equalTo(expectedCountsByKey.get(key)));
    });
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void reverseToRootGroupByOnNestedFieldWithFilterTestWithReverseNestedAndEmptyPath()
      throws Exception {

    String query = String.format("SELECT COUNT(*) FROM %s GROUP BY  nested(message.info)," +
            "filter('myFilter',message.info = 'a'),reverse_nested(someField,'')",
        TEST_INDEX_NESTED_TYPE);
    JSONObject result = executeQuery(query);
    JSONObject aggregation = getAggregation(result, "message.info@NESTED");

    JSONArray msgInfoBuckets =
        (JSONArray) aggregation.optQuery("/myFilter@FILTER/message.info/buckets");
    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(msgInfoBuckets.length(), equalTo(1));

    JSONArray someFieldBuckets =
        (JSONArray) msgInfoBuckets.optQuery("/0/someField@NESTED/someField/buckets");
    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(msgInfoBuckets.length(), equalTo(1));
    Assert.assertThat(someFieldBuckets.query("/0/key"), equalTo("b"));
    Assert.assertThat(someFieldBuckets.query("/0/COUNT(*)/value"), equalTo(2));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void reverseToRootGroupByOnNestedFieldWithFilterTestWithReverseNestedNoPath()
      throws Exception {

    String query = String.format("SELECT COUNT(*) FROM %s GROUP BY  nested(message.info),filter" +
        "('myFilter',message.info = 'a'),reverse_nested(someField)", TEST_INDEX_NESTED_TYPE);
    JSONObject result = executeQuery(query);
    JSONObject aggregation = getAggregation(result, "message.info@NESTED");

    JSONArray msgInfoBuckets =
        (JSONArray) aggregation.optQuery("/myFilter@FILTER/message.info/buckets");
    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(msgInfoBuckets.length(), equalTo(1));

    JSONArray someFieldBuckets =
        (JSONArray) msgInfoBuckets.optQuery("/0/someField@NESTED/someField/buckets");
    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(someFieldBuckets.length(), equalTo(1));
    Assert.assertThat(someFieldBuckets.query("/0/key"), equalTo("b"));
    Assert.assertThat(someFieldBuckets.query("/0/COUNT(*)/value"), equalTo(2));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void reverseToRootGroupByOnNestedFieldWithFilterTestWithReverseNestedOnHistogram()
      throws Exception {

    String query = String.format("SELECT COUNT(*) FROM %s GROUP BY  nested(message.info)," +
        "filter('myFilter',message.info = 'a'),histogram('field'='myNum','reverse_nested'='','interval'='2', " +
        "'alias' = 'someAlias' )", TEST_INDEX_NESTED_TYPE);
    JSONObject result = executeQuery(query);
    JSONObject aggregation = getAggregation(result, "message.info@NESTED");

    JSONArray msgInfoBuckets =
        (JSONArray) aggregation.optQuery("/myFilter@FILTER/message.info/buckets");
    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(msgInfoBuckets.length(), equalTo(1));

    JSONArray someAliasBuckets =
        (JSONArray) msgInfoBuckets.optQuery("/0/someAlias@NESTED/someAlias/buckets");
    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(someAliasBuckets.length(), equalTo(3));

    final Map<Double, Integer> expectedCountsByKey = new HashMap<>();
    expectedCountsByKey.put(0.0, 1);
    expectedCountsByKey.put(2.0, 0);
    expectedCountsByKey.put(4.0, 1);

    someAliasBuckets.forEach(obj -> {
      JSONObject bucket = (JSONObject) obj;
      final double key = bucket.getDouble("key");
      Assert.assertTrue(expectedCountsByKey.containsKey(key));
      Assert.assertThat(bucket.getJSONObject("COUNT(*)").getInt("value"),
          equalTo(expectedCountsByKey.get(key)));
    });
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void reverseToRootGroupByOnNestedFieldWithFilterAndSumOnReverseNestedField()
      throws Exception {

    String query = String.format("SELECT sum(reverse_nested(myNum)) bla FROM %s GROUP BY " +
        "nested(message.info),filter('myFilter',message.info = 'a')", TEST_INDEX_NESTED_TYPE);
    JSONObject result = executeQuery(query);
    JSONObject aggregation = getAggregation(result, "message.info@NESTED");

    JSONArray msgInfoBuckets =
        (JSONArray) aggregation.optQuery("/myFilter@FILTER/message.info/buckets");
    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(msgInfoBuckets.length(), equalTo(1));

    Assert.assertNotNull(msgInfoBuckets.optQuery("/0/myNum@NESTED/bla/value"));
    JSONObject bla = (JSONObject) msgInfoBuckets.query("/0/myNum@NESTED/bla");
    Assert.assertEquals(5.0, bla.getDouble("value"), 0.000001);
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void reverseAnotherNestedGroupByOnNestedFieldWithFilterTestWithReverseNestedNoPath()
      throws Exception {

    String query = String.format("SELECT COUNT(*) FROM %s GROUP BY  nested(message.info)," +
            "filter('myFilter',message.info = 'a'),reverse_nested(comment.data,'~comment')",
        TEST_INDEX_NESTED_TYPE);
    JSONObject result = executeQuery(query);
    JSONObject aggregation = getAggregation(result, "message.info@NESTED");

    JSONArray msgInfoBuckets =
        (JSONArray) aggregation.optQuery("/myFilter@FILTER/message.info/buckets");
    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(msgInfoBuckets.length(), equalTo(1));

    JSONArray commentDataBuckets =
        (JSONArray) msgInfoBuckets.optQuery("/0/comment.data@NESTED_REVERSED" +
            "/comment.data@NESTED/comment.data/buckets");
    Assert.assertNotNull(commentDataBuckets);
    Assert.assertThat(commentDataBuckets.length(), equalTo(1));
    Assert.assertThat(commentDataBuckets.query("/0/key"), equalTo("ab"));
    Assert.assertThat(commentDataBuckets.query("/0/COUNT(*)/value"), equalTo(2));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void reverseAnotherNestedGroupByOnNestedFieldWithFilterTestWithReverseNestedOnHistogram()
      throws Exception {

    String query = String.format("SELECT COUNT(*) FROM %s GROUP BY  nested(message.info),filter" +
        "('myFilter',message.info = 'a'),histogram('field'='comment.likes','reverse_nested'='~comment'," +
        "'interval'='2' , 'alias' = 'someAlias' )", TEST_INDEX_NESTED_TYPE);
    JSONObject result = executeQuery(query);
    JSONObject aggregation = getAggregation(result, "message.info@NESTED");

    JSONArray msgInfoBuckets =
        (JSONArray) aggregation.optQuery("/myFilter@FILTER/message.info/buckets");
    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(msgInfoBuckets.length(), equalTo(1));

    JSONArray someAliasBuckets = (JSONArray) msgInfoBuckets.optQuery(
        "/0/~comment@NESTED_REVERSED/~comment@NESTED/someAlias/buckets");
    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(someAliasBuckets.length(), equalTo(2));

    final Map<Double, Integer> expectedCountsByKey = new HashMap<>();
    expectedCountsByKey.put(0.0, 1);
    expectedCountsByKey.put(2.0, 1);

    someAliasBuckets.forEach(obj -> {
      JSONObject bucket = (JSONObject) obj;
      final double key = bucket.getDouble("key");
      Assert.assertTrue(expectedCountsByKey.containsKey(key));
      Assert.assertThat(bucket.getJSONObject("COUNT(*)").getInt("value"),
          equalTo(expectedCountsByKey.get(key)));
    });
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void reverseAnotherNestedGroupByOnNestedFieldWithFilterAndSumOnReverseNestedField()
      throws Exception {

    String query =
        String.format("SELECT sum(reverse_nested(comment.likes,'~comment')) bla FROM %s " +
                "GROUP BY  nested(message.info),filter('myFilter',message.info = 'a')",
            TEST_INDEX_NESTED_TYPE);
    JSONObject result = executeQuery(query);
    JSONObject aggregation = getAggregation(result, "message.info@NESTED");

    JSONArray msgInfoBuckets =
        (JSONArray) aggregation.optQuery("/myFilter@FILTER/message.info/buckets");
    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(msgInfoBuckets.length(), equalTo(1));

    Assert.assertNotNull(msgInfoBuckets.optQuery(
        "/0/comment.likes@NESTED_REVERSED/comment.likes@NESTED/bla/value"));
    JSONObject bla = (JSONObject) msgInfoBuckets
        .query("/0/comment.likes@NESTED_REVERSED/comment.likes@NESTED/bla");
    Assert.assertEquals(4.0, bla.getDouble("value"), 0.000001);
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void docsReturnedTestWithoutDocsHint() throws Exception {
    String query = String.format("SELECT count(*) from %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeQuery(query);
    Assert.assertThat(getHits(result).length(), equalTo(0));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void docsReturnedTestWithDocsHint() throws Exception {
    String query = String.format("SELECT /*! DOCS_WITH_AGGREGATION(10) */ count(*) from %s",
        TEST_INDEX_ACCOUNT);
    JSONObject result = executeQuery(query);
    Assert.assertThat(getHits(result).length(), equalTo(10));
  }

  @Ignore("There is not any text field in the index. Need fix later")
  @org.junit.Test
  public void termsWithScript() throws Exception {
    String query =
        String.format("select count(*), avg(all_client) from %s group by terms('alias'='asdf'," +
            " substring(field, 0, 1)), date_histogram('alias'='time', 'field'='timestamp', " +
            "'interval'='20d ', 'format'='yyyy-MM-dd') limit 1000", TEST_INDEX_ONLINE);
    String result = explainQuery(query);

    Assert.assertThat(result, containsString("\"script\":{\"source\""));
    Assert.assertThat(result, containsString("substring(0, 1)"));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void groupByScriptedDateHistogram() throws Exception {
    String query = String
        .format("select count(*), avg(all_client) from %s group by date_histogram('alias'='time'," +
                " ceil(all_client), 'fixed_interval'='20d ', 'format'='yyyy-MM-dd') limit 1000",
            TEST_INDEX_ONLINE);
    String result = explainQuery(query);

    Assert.assertThat(result, containsString("Math.ceil(doc['all_client'].value);"));
    Assert.assertThat(result, containsString("\"script\":{\"source\""));
  }

  @Ignore("Aggregation is not supported with JSON format in the new engine")
  @org.junit.Test
  public void groupByScriptedHistogram() throws Exception {
    String query = String.format(
        "select count(*) from %s group by histogram('alias'='all_field', pow(all_client,1))",
        TEST_INDEX_ONLINE);
    String result = explainQuery(query);

    Assert.assertThat(result, containsString("Math.pow(doc['all_client'].value, 1)"));
    Assert.assertThat(result, containsString("\"script\":{\"source\""));
  }

  @org.junit.Test
  public void distinctWithOneField() {
    Assert.assertEquals(
        executeQuery("SELECT DISTINCT name.lastname FROM " + TEST_INDEX_GAME_OF_THRONES, "jdbc"),
        executeQuery("SELECT name.lastname FROM " + TEST_INDEX_GAME_OF_THRONES
            + " GROUP BY name.lastname", "jdbc")
    );
  }

  @org.junit.Test
  public void distinctWithMultipleFields() {
    Assert.assertEquals(
        executeQuery("SELECT DISTINCT age, gender FROM " + TEST_INDEX_ACCOUNT, "jdbc"),
        executeQuery("SELECT age, gender FROM " + TEST_INDEX_ACCOUNT
            + " GROUP BY age, gender", "jdbc")
    );
  }

  private JSONObject getAggregation(final JSONObject queryResult, final String aggregationName) {
    final String aggregationsObjName = "aggregations";
    Assert.assertTrue(queryResult.has(aggregationsObjName));

    final JSONObject aggregations = queryResult.getJSONObject(aggregationsObjName);
    Assert.assertTrue(aggregations.has(aggregationName));
    return aggregations.getJSONObject(aggregationName);
  }

  private int getIntAggregationValue(final JSONObject queryResult, final String aggregationName,
                                     final String fieldName) {

    final JSONObject targetAggregation = getAggregation(queryResult, aggregationName);
    Assert.assertTrue(targetAggregation.has(fieldName));
    return targetAggregation.getInt(fieldName);
  }

  private double getDoubleAggregationValue(final JSONObject queryResult,
                                           final String aggregationName,
                                           final String fieldName) {

    final JSONObject targetAggregation = getAggregation(queryResult, aggregationName);
    Assert.assertTrue(targetAggregation.has(fieldName));
    return targetAggregation.getDouble(fieldName);
  }

  private double getDoubleAggregationValue(final JSONObject queryResult,
                                           final String aggregationName,
                                           final String fieldName, final String subFieldName) {

    final JSONObject targetAggregation = getAggregation(queryResult, aggregationName);
    Assert.assertTrue(targetAggregation.has(fieldName));
    final JSONObject targetField = targetAggregation.getJSONObject(fieldName);
    Assert.assertTrue(targetField.has(subFieldName));

    return targetField.getDouble(subFieldName);
  }

  protected JSONObject executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
