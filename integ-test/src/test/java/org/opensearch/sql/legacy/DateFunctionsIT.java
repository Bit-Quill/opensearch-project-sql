/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.matchesPattern;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONObject;
import org.junit.Test;

public class DateFunctionsIT extends SQLIntegTestCase {

  private static final String FROM = "FROM " + TestsConstants.TEST_INDEX_ONLINE;

  /**
   * Some of the first few SQL functions are tested in both SELECT and WHERE cases for flexibility and the remainder
   * are merely tested in SELECT for simplicity.
   * <p>
   * There is a limitation in all date SQL functions in that they expect a date field as input. In the future this
   * can be expanded on by supporting CAST and casting dates given as Strings to TIMESTAMP (SQL's date type).
   */

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ONLINE);
  }

  @Test
  public void year() {
    String query = "SELECT YEAR(insert_time) as year " + FROM;
    JSONObject result = executeJdbcRequest(query);
    assertEquals(9936, result.getInt("total"));
    verifySchema(result, schema("YEAR(insert_time)", "year", "integer"));
    assertEquals(2014, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void monthOfYear() {
    String query = "SELECT MONTH_OF_YEAR(insert_time) as month_of_year " + FROM;
    JSONObject result = executeJdbcRequest(query);
    assertEquals(9936, result.getInt("total"));
    verifySchema(result, schema("MONTH_OF_YEAR(insert_time)", "month_of_year", "integer"));
    assertEquals(8, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void weekOfYearInSelect() {
    String query = "SELECT WEEK_OF_YEAR(insert_time) as week_of_year " + FROM;
    JSONObject result = executeJdbcRequest(query);
    assertEquals(9936, result.getInt("total"));
    verifySchema(result, schema("WEEK_OF_YEAR(insert_time)", "week_of_year", "integer"));
    assertEquals(33, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void weekOfYearInWhere() {
    String query = "SELECT insert_time " + FROM
            + " WHERE DATE_FORMAT(insert_time, 'YYYY-MM-dd') < '2014-08-19' AND " +
            "WEEK_OF_YEAR(insert_time) > 33 LIMIT 2000";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(0, result.getInt("total"));
    verifySchema(result, schema("insert_time", "timestamp"));
  }

  @Test
  public void dayOfYearInSelect() {
    String query = "SELECT DAY_OF_YEAR(insert_time) as day_of_year " + FROM + " LIMIT 2000";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(2000, result.getInt("total"));
    verifySchema(result, schema("DAY_OF_YEAR(insert_time)", "day_of_year", "integer"));
    assertEquals(229, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void dayOfYearInWhere() {
    String query = "SELECT insert_time " + FROM
            + " WHERE DAY_OF_YEAR(insert_time) < 233 LIMIT 10000";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(4721, result.getInt("total"));
    verifySchema(result, schema("insert_time", "timestamp"));
    assertEquals("2014-08-17 16:00:05.442", result.getJSONArray("datarows").getJSONArray(0).getString(0));
  }

  @Test
  public void dayOfMonthInSelect() {
    String query = "SELECT DAY_OF_MONTH(insert_time) as day_of_month " + FROM
            + " LIMIT 2000";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(2000, result.getInt("total"));
    verifySchema(result, schema("DAY_OF_MONTH(insert_time)", "day_of_month", "integer"));
    assertEquals(17, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void dayOfMonthInWhere() {
    String query = "SELECT insert_time " + FROM
            + " WHERE DAY_OF_MONTH(insert_time) < 21 LIMIT 10000";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(4721, result.getInt("total"));
    verifySchema(result, schema("insert_time", "timestamp"));
    assertEquals("2014-08-17 16:00:05.442", result.getJSONArray("datarows").getJSONArray(0).getString(0));
  }

  @Test
  public void dayOfWeek() {
    String query = "SELECT DAY_OF_WEEK(insert_time) as day_of_week " + FROM
            + " LIMIT 2000";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(2000, result.getInt("total"));
    verifySchema(result, schema("DAY_OF_WEEK(insert_time)", "day_of_week", "integer"));
    assertEquals(1, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void hourOfDay() {
    String query = "SELECT HOUR_OF_DAY(insert_time) as hour_of_day " + FROM
            + " LIMIT 1000";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("HOUR_OF_DAY(insert_time)", "hour_of_day", "integer"));
    assertEquals(16, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void minuteOfDay() {
    String query = "SELECT MINUTE_OF_DAY(insert_time) as minute_of_day " + FROM
            + " LIMIT 500";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(500, result.getInt("total"));
    verifySchema(result, schema("MINUTE_OF_DAY(insert_time)", "minute_of_day", "integer"));
    assertEquals(960, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void minuteOfHour() {
    String query = "SELECT MINUTE_OF_HOUR(insert_time) as minute_of_hour " + FROM
            + " LIMIT 500";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(500, result.getInt("total"));
    verifySchema(result, schema("MINUTE_OF_HOUR(insert_time)", "minute_of_hour", "integer"));
    assertEquals(0, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void secondOfMinute() {
    String query = "SELECT SECOND_OF_MINUTE(insert_time) as second_of_minute " + FROM
            + " LIMIT 500";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(500, result.getInt("total"));
    verifySchema(result, schema("SECOND_OF_MINUTE(insert_time)", "second_of_minute", "integer"));
    assertEquals(5, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void month() {
    String query = "SELECT MONTH(insert_time) as month " + FROM + " LIMIT 500";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(500, result.getInt("total"));
    verifySchema(result, schema("MONTH(insert_time)", "month", "integer"));
    assertEquals(8, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void dayofmonth() {
    String query = "SELECT DAYOFMONTH(insert_time) as dayofmonth " + FROM + " LIMIT 500";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(500, result.getInt("total"));
    verifySchema(result, schema("DAYOFMONTH(insert_time)", "dayofmonth", "integer"));
    assertEquals(17, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void date() {
    String query = "SELECT DATE(insert_time) as date " + FROM + " LIMIT 500";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(500, result.getInt("total"));
    verifySchema(result, schema("DATE(insert_time)", "date", "date"));
    assertEquals("2014-08-17", result.getJSONArray("datarows").getJSONArray(0).getString(0));
  }

  @Test
  public void monthname() {
    String query = "SELECT MONTHNAME(insert_time) AS monthname " + FROM + " LIMIT 500";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(500, result.getInt("total"));
    verifySchema(result, schema("MONTHNAME(insert_time)", "monthname", "keyword"));
    assertEquals("August", result.getJSONArray("datarows").getJSONArray(0).getString(0));
  }

  @Test
  public void timestamp() {
    String query = "SELECT TIMESTAMP(insert_time) AS timestamp " + FROM + " LIMIT 500";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(500, result.getInt("total"));
    verifySchema(result, schema("TIMESTAMP(insert_time)", "timestamp", "timestamp"));
    assertEquals("2014-08-17 16:00:05.442", result.getJSONArray("datarows").getJSONArray(0).getString(0));
  }

  @Test
  public void maketime() {
    String query = "SELECT MAKETIME(13, 1, 1) AS maketime " + FROM + " LIMIT 1";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1, result.getInt("total"));
    verifySchema(result, schema("MAKETIME(13, 1, 1)", "maketime", "time"));
    verifyDataRows(result, rows("13:01:01"));
  }

  @Test
  public void now() {
    String query = "SELECT NOW() AS now " + FROM + " LIMIT 1";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1, result.getInt("total"));
    verifySchema(result, schema("NOW()", "now", "datetime"));

    String now =  result
            .getJSONArray("datarows").getJSONArray(0).getString(0).split(" ")[1];
    assertThat(now, matchesPattern("[0-9]{2}:[0-9]{2}:[0-9]{2}"));
  }

  @Test
  public void curdate() {
    String query = "SELECT CURDATE() AS curdate " + FROM + " LIMIT 1";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1, result.getInt("total"));
    verifySchema(result, schema("CURDATE()", "curdate", "date"));

    String curdate = result.getJSONArray("datarows").getJSONArray(0).getString(0);
    assertThat(curdate, matchesPattern("[0-9]{4}-[0-9]{2}-[0-9]{2}"));
  }
}
