/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Locale;
import java.util.TimeZone;

import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class DateTimeComparisonIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
  }

  private final TimeZone testTz = TimeZone.getDefault();
  private final TimeZone systemTz = TimeZone.getTimeZone(System.getProperty("user.timezone"));

  @Before
  public void setTimeZone() {
    TimeZone.setDefault(systemTz);
  }

  @After
  public void resetTimeZone() {
    TimeZone.setDefault(testTz);
  }

  @Test
  public void testCompareDates() throws IOException {
    JSONObject result = executeQuery("select "
        + "DATE('2020-09-16') = DATE('2020-09-16') AS `eq1`, "
        + "DATE('2020-09-16') = DATE('1961-04-12') AS `eq2`, "
        + "DATE('2020-09-16') != DATE('1984-12-15') AS `neq1`, "
        + "DATE('1961-04-12') != DATE('1984-12-15') AS `neq2`, "
        + "DATE('1961-04-12') != DATE('1961-04-12') AS `neq3`, "
        + "DATE('1984-12-15') > DATE('1961-04-12') AS `gt1`, "
        + "DATE('1984-12-15') > DATE('2020-09-16') AS `gt2`, "
        + "DATE('1961-04-12') < DATE('1984-12-15') AS `lt1`, "
        + "DATE('1984-12-15') < DATE('1961-04-12') AS `lt2`, "
        + "DATE('1984-12-15') >= DATE('1961-04-12') AS `gte1`, "
        + "DATE('1984-12-15') >= DATE('1984-12-15') AS `gte2`, "
        + "DATE('1984-12-15') >= DATE('2020-09-16') AS `gte3`, "
        + "DATE('1961-04-12') <= DATE('1984-12-15') AS `lte1`, "
        + "DATE('1961-04-12') <= DATE('1961-04-12') AS `lte2`, "
        + "DATE('2020-09-16') <= DATE('1961-04-12') AS `lte3`");
    verifySchema(result,
        schema("DATE('2020-09-16') = DATE('2020-09-16')", "eq1", "boolean"),
        schema("DATE('2020-09-16') = DATE('1961-04-12')", "eq2", "boolean"),
        schema("DATE('2020-09-16') != DATE('1984-12-15')", "neq1", "boolean"),
        schema("DATE('1961-04-12') != DATE('1984-12-15')", "neq2", "boolean"),
        schema("DATE('1961-04-12') != DATE('1961-04-12')", "neq3", "boolean"),
        schema("DATE('1984-12-15') > DATE('1961-04-12')", "gt1", "boolean"),
        schema("DATE('1984-12-15') > DATE('2020-09-16')", "gt2", "boolean"),
        schema("DATE('1961-04-12') < DATE('1984-12-15')", "lt1", "boolean"),
        schema("DATE('1984-12-15') < DATE('1961-04-12')", "lt2", "boolean"),
        schema("DATE('1984-12-15') >= DATE('1961-04-12')", "gte1", "boolean"),
        schema("DATE('1984-12-15') >= DATE('1984-12-15')", "gte2", "boolean"),
        schema("DATE('1984-12-15') >= DATE('2020-09-16')", "gte3", "boolean"),
        schema("DATE('1961-04-12') <= DATE('1984-12-15')", "lte1", "boolean"),
        schema("DATE('1961-04-12') <= DATE('1961-04-12')", "lte2", "boolean"),
        schema("DATE('2020-09-16') <= DATE('1961-04-12')", "lte3", "boolean"));
    verifyDataRows(result,
        rows(true, false, true, true, false, true, false,
            true, false, true, true, false, true, true, false));
  }

  @Test
  public void testCompareTimes() throws IOException {
    JSONObject result = executeQuery("select "
        + "TIME('09:16:37') = TIME('09:16:37') AS `eq1`, "
        + "TIME('09:16:37') = TIME('04:12:42') AS `eq2`, "
        + "TIME('09:16:37') != TIME('12:15:22') AS `neq1`, "
        + "TIME('04:12:42') != TIME('12:15:22') AS `neq2`, "
        + "TIME('04:12:42') != TIME('04:12:42') AS `neq3`, "
        + "TIME('12:15:22') > TIME('04:12:42') AS `gt1`, "
        + "TIME('12:15:22') > TIME('19:16:03') AS `gt2`, "
        + "TIME('04:12:42') < TIME('12:15:22') AS `lt1`, "
        + "TIME('14:12:38') < TIME('12:15:22') AS `lt2`, "
        + "TIME('12:15:22') >= TIME('04:12:42') AS `gte1`, "
        + "TIME('12:15:22') >= TIME('12:15:22') AS `gte2`, "
        + "TIME('12:15:22') >= TIME('19:16:03') AS `gte3`, "
        + "TIME('04:12:42') <= TIME('12:15:22') AS `lte1`, "
        + "TIME('04:12:42') <= TIME('04:12:42') AS `lte2`, "
        + "TIME('19:16:03') <= TIME('04:12:42') AS `lte3` ");
    verifySchema(result,
        schema("TIME('09:16:37') = TIME('09:16:37')", "eq1", "boolean"),
        schema("TIME('09:16:37') = TIME('04:12:42')", "eq2", "boolean"),
        schema("TIME('09:16:37') != TIME('12:15:22')", "neq1", "boolean"),
        schema("TIME('04:12:42') != TIME('12:15:22')", "neq2", "boolean"),
        schema("TIME('04:12:42') != TIME('04:12:42')", "neq3", "boolean"),
        schema("TIME('12:15:22') > TIME('04:12:42')", "gt1", "boolean"),
        schema("TIME('12:15:22') > TIME('19:16:03')", "gt2", "boolean"),
        schema("TIME('04:12:42') < TIME('12:15:22')", "lt1", "boolean"),
        schema("TIME('14:12:38') < TIME('12:15:22')", "lt2", "boolean"),
        schema("TIME('12:15:22') >= TIME('04:12:42')", "gte1", "boolean"),
        schema("TIME('12:15:22') >= TIME('12:15:22')", "gte2", "boolean"),
        schema("TIME('12:15:22') >= TIME('19:16:03')", "gte3", "boolean"),
        schema("TIME('04:12:42') <= TIME('12:15:22')", "lte1", "boolean"),
        schema("TIME('04:12:42') <= TIME('04:12:42')", "lte2", "boolean"),
        schema("TIME('19:16:03') <= TIME('04:12:42')", "lte3", "boolean"));
    verifyDataRows(result,
        rows(true, false, true, true, false, true, false,
            true, false, true, true, false, true, true, false));
  }

  @Test
  public void testCompareDatetimes() throws IOException {
    JSONObject result = executeQuery("select "
        + "DATETIME('2020-09-16 10:20:30') = DATETIME('2020-09-16 10:20:30') AS `eq1`, "
        + "DATETIME('2020-09-16 10:20:30') = DATETIME('1961-04-12 09:07:00') AS `eq2`, "
        + "DATETIME('2020-09-16 10:20:30') != DATETIME('1984-12-15 22:15:07') AS `neq1`, "
        + "DATETIME('1984-12-15 22:15:08') != DATETIME('1984-12-15 22:15:07') AS `neq2`, "
        + "DATETIME('1961-04-12 09:07:00') != DATETIME('1961-04-12 09:07:00') AS `neq3`, "
        + "DATETIME('1984-12-15 22:15:07') > DATETIME('1961-04-12 22:15:07') AS `gt1`, "
        + "DATETIME('1984-12-15 22:15:07') > DATETIME('1984-12-15 22:15:06') AS `gt2`, "
        + "DATETIME('1984-12-15 22:15:07') > DATETIME('2020-09-16 10:20:30') AS `gt3`, "
        + "DATETIME('1961-04-12 09:07:00') < DATETIME('1984-12-15 09:07:00') AS `lt1`, "
        + "DATETIME('1984-12-15 22:15:07') < DATETIME('1984-12-15 22:15:08') AS `lt2`, "
        + "DATETIME('1984-12-15 22:15:07') < DATETIME('1961-04-12 09:07:00') AS `lt3`, "
        + "DATETIME('1984-12-15 22:15:07') >= DATETIME('1961-04-12 09:07:00') AS `gte1`, "
        + "DATETIME('1984-12-15 22:15:07') >= DATETIME('1984-12-15 22:15:07') AS `gte2`, "
        + "DATETIME('1984-12-15 22:15:07') >= DATETIME('2020-09-16 10:20:30') AS `gte3`, "
        + "DATETIME('1961-04-12 09:07:00') <= DATETIME('1984-12-15 22:15:07') AS `lte1`, "
        + "DATETIME('1961-04-12 09:07:00') <= DATETIME('1961-04-12 09:07:00') AS `lte2`, "
        + "DATETIME('2020-09-16 10:20:30') <= DATETIME('1961-04-12 09:07:00') AS `lte3` ");
    verifySchema(result,
        schema("DATETIME('2020-09-16 10:20:30') = DATETIME('2020-09-16 10:20:30')", "eq1", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') = DATETIME('1961-04-12 09:07:00')", "eq2", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') != DATETIME('1984-12-15 22:15:07')", "neq1", "boolean"),
        schema("DATETIME('1984-12-15 22:15:08') != DATETIME('1984-12-15 22:15:07')", "neq2", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') != DATETIME('1961-04-12 09:07:00')", "neq3", "boolean"),
        schema("DATETIME('1984-12-15 22:15:07') > DATETIME('1961-04-12 22:15:07')", "gt1", "boolean"),
        schema("DATETIME('1984-12-15 22:15:07') > DATETIME('1984-12-15 22:15:06')", "gt2", "boolean"),
        schema("DATETIME('1984-12-15 22:15:07') > DATETIME('2020-09-16 10:20:30')", "gt3", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') < DATETIME('1984-12-15 09:07:00')", "lt1", "boolean"),
        schema("DATETIME('1984-12-15 22:15:07') < DATETIME('1984-12-15 22:15:08')", "lt2", "boolean"),
        schema("DATETIME('1984-12-15 22:15:07') < DATETIME('1961-04-12 09:07:00')", "lt3", "boolean"),
        schema("DATETIME('1984-12-15 22:15:07') >= DATETIME('1961-04-12 09:07:00')", "gte1", "boolean"),
        schema("DATETIME('1984-12-15 22:15:07') >= DATETIME('1984-12-15 22:15:07')", "gte2", "boolean"),
        schema("DATETIME('1984-12-15 22:15:07') >= DATETIME('2020-09-16 10:20:30')", "gte3", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') <= DATETIME('1984-12-15 22:15:07')", "lte1", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') <= DATETIME('1961-04-12 09:07:00')", "lte2", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') <= DATETIME('1961-04-12 09:07:00')", "lte3", "boolean"));
    verifyDataRows(result,
        rows(true, false, true, true, false, true, true, false,
            true, true, false, true, true, false, true, true, false));
  }

  @Test
  public void testCompareTimestamps() throws IOException {
    JSONObject result = executeQuery("select "
        + "TIMESTAMP('2020-09-16 10:20:30') = TIMESTAMP('2020-09-16 10:20:30') AS `eq1`, "
        + "TIMESTAMP('2020-09-16 10:20:30') = TIMESTAMP('1961-04-12 09:07:00') AS `eq2`, "
        + "TIMESTAMP('2020-09-16 10:20:30') != TIMESTAMP('1984-12-15 22:15:07') AS `neq1`, "
        + "TIMESTAMP('1984-12-15 22:15:08') != TIMESTAMP('1984-12-15 22:15:07') AS `neq2`, "
        + "TIMESTAMP('1961-04-12 09:07:00') != TIMESTAMP('1961-04-12 09:07:00') AS `neq3`, "
        + "TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('1961-04-12 22:15:07') AS `gt1`, "
        + "TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('1984-12-15 22:15:06') AS `gt2`, "
        + "TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('2020-09-16 10:20:30') AS `gt3`, "
        + "TIMESTAMP('1961-04-12 09:07:00') < TIMESTAMP('1984-12-15 09:07:00') AS `lt1`, "
        + "TIMESTAMP('1984-12-15 22:15:07') < TIMESTAMP('1984-12-15 22:15:08') AS `lt2`, "
        + "TIMESTAMP('1984-12-15 22:15:07') < TIMESTAMP('1961-04-12 09:07:00') AS `lt3`, "
        + "TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('1961-04-12 09:07:00') AS `gte1`, "
        + "TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('1984-12-15 22:15:07') AS `gte2`, "
        + "TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('2020-09-16 10:20:30') AS `gte3`, "
        + "TIMESTAMP('1961-04-12 09:07:00') <= TIMESTAMP('1984-12-15 22:15:07') AS `lte1`, "
        + "TIMESTAMP('1961-04-12 09:07:00') <= TIMESTAMP('1961-04-12 09:07:00') AS `lte2`, "
        + "TIMESTAMP('2020-09-16 10:20:30') <= TIMESTAMP('1961-04-12 09:07:00') AS `lte3` ");
    verifySchema(result,
        schema("TIMESTAMP('2020-09-16 10:20:30') = TIMESTAMP('2020-09-16 10:20:30')", "eq1", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') = TIMESTAMP('1961-04-12 09:07:00')", "eq2", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') != TIMESTAMP('1984-12-15 22:15:07')", "neq1", "boolean"),
        schema("TIMESTAMP('1984-12-15 22:15:08') != TIMESTAMP('1984-12-15 22:15:07')", "neq2", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') != TIMESTAMP('1961-04-12 09:07:00')", "neq3", "boolean"),
        schema("TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('1961-04-12 22:15:07')", "gt1", "boolean"),
        schema("TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('1984-12-15 22:15:06')", "gt2", "boolean"),
        schema("TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('2020-09-16 10:20:30')", "gt3", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') < TIMESTAMP('1984-12-15 09:07:00')", "lt1", "boolean"),
        schema("TIMESTAMP('1984-12-15 22:15:07') < TIMESTAMP('1984-12-15 22:15:08')", "lt2", "boolean"),
        schema("TIMESTAMP('1984-12-15 22:15:07') < TIMESTAMP('1961-04-12 09:07:00')", "lt3", "boolean"),
        schema("TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('1961-04-12 09:07:00')", "gte1", "boolean"),
        schema("TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('1984-12-15 22:15:07')", "gte2", "boolean"),
        schema("TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('2020-09-16 10:20:30')", "gte3", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') <= TIMESTAMP('1984-12-15 22:15:07')", "lte1", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') <= TIMESTAMP('1961-04-12 09:07:00')", "lte2", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') <= TIMESTAMP('1961-04-12 09:07:00')", "lte3", "boolean"));
    verifyDataRows(result,
        rows(true, false, true, true, false, true, true, false,
            true, true, false, true, true, false, true, true, false));
  }

  @Test
  public void testEqCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIMESTAMP('2020-09-16 10:20:30') = DATETIME('2020-09-16 10:20:30') AS `ts_dt_t`, "
        + "DATETIME('2020-09-16 10:20:30') = TIMESTAMP('2020-09-16 10:20:30') AS `dt_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') = DATETIME('1961-04-12 09:07:00') AS `ts_dt_f`, "
        + "DATETIME('1961-04-12 09:07:00') = TIMESTAMP('1984-12-15 22:15:07') AS `dt_ts_f`, "
        + "TIMESTAMP('2020-09-16 00:00:00') = DATE('2020-09-16') AS `ts_d_t`, "
        + "DATE('2020-09-16') = TIMESTAMP('2020-09-16 00:00:00') AS `d_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') = DATE('1961-04-12') AS `ts_d_f`, "
        + "DATE('1961-04-12') = TIMESTAMP('1984-12-15 22:15:07') AS `d_ts_f`, "
        + "TIMESTAMP('" + today + " 10:20:30') = TIME('10:20:30') AS `ts_t_t`, "
        + "TIME('10:20:30') = TIMESTAMP('" + today + " 10:20:30') AS `t_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') = TIME('09:07:00') AS `ts_t_f`, "
        + "TIME('09:07:00') = TIMESTAMP('1984-12-15 22:15:07') AS `t_ts_f` ");
    verifySchema(result,
        schema("TIMESTAMP('2020-09-16 10:20:30') = DATETIME('2020-09-16 10:20:30')", "ts_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') = TIMESTAMP('2020-09-16 10:20:30')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') = DATETIME('1961-04-12 09:07:00')", "ts_dt_f", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') = TIMESTAMP('1984-12-15 22:15:07')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 00:00:00') = DATE('2020-09-16')", "ts_d_t", "boolean"),
        schema("DATE('2020-09-16') = TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') = DATE('1961-04-12')", "ts_d_f", "boolean"),
        schema("DATE('1961-04-12') = TIMESTAMP('1984-12-15 22:15:07')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('" + today + " 10:20:30') = TIME('10:20:30')", "ts_t_t", "boolean"),
        schema("TIME('10:20:30') = TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') = TIME('09:07:00')", "ts_t_f", "boolean"),
        schema("TIME('09:07:00') = TIMESTAMP('1984-12-15 22:15:07')", "t_ts_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testEqCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "DATETIME('2020-09-16 10:20:30') = TIMESTAMP('2020-09-16 10:20:30') AS `dt_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') = DATETIME('2020-09-16 10:20:30') AS `ts_dt_t`, "
        + "DATETIME('2020-09-16 10:20:30') = TIMESTAMP('1961-04-12 09:07:00') AS `dt_ts_f`, "
        + "TIMESTAMP('1961-04-12 09:07:00') = DATETIME('1984-12-15 22:15:07') AS `ts_dt_f`, "
        + "DATETIME('2020-09-16 00:00:00') = DATE('2020-09-16') AS `dt_d_t`, "
        + "DATE('2020-09-16') = DATETIME('2020-09-16 00:00:00') AS `d_dt_t`, "
        + "DATETIME('2020-09-16 10:20:30') = DATE('1961-04-12') AS `dt_d_f`, "
        + "DATE('1961-04-12') = DATETIME('1984-12-15 22:15:07') AS `d_dt_f`, "
        + "DATETIME('" + today + " 10:20:30') = TIME('10:20:30') AS `dt_t_t`, "
        + "TIME('10:20:30') = DATETIME('" + today + " 10:20:30') AS `t_dt_t`, "
        + "DATETIME('2020-09-16 10:20:30') = TIME('09:07:00') AS `dt_t_f`, "
        + "TIME('09:07:00') = DATETIME('1984-12-15 22:15:07') AS `t_dt_f` ");
    verifySchema(result,
        schema("DATETIME('2020-09-16 10:20:30') = TIMESTAMP('2020-09-16 10:20:30')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') = DATETIME('2020-09-16 10:20:30')", "ts_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') = TIMESTAMP('1961-04-12 09:07:00')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') = DATETIME('1984-12-15 22:15:07')", "ts_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') = DATE('2020-09-16')", "dt_d_t", "boolean"),
        schema("DATE('2020-09-16') = DATETIME('2020-09-16 00:00:00')", "d_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') = DATE('1961-04-12')", "dt_d_f", "boolean"),
        schema("DATE('1961-04-12') = DATETIME('1984-12-15 22:15:07')", "d_dt_f", "boolean"),
        schema("DATETIME('" + today + " 10:20:30') = TIME('10:20:30')", "dt_t_t", "boolean"),
        schema("TIME('10:20:30') = DATETIME('" + today + " 10:20:30')", "t_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') = TIME('09:07:00')", "dt_t_f", "boolean"),
        schema("TIME('09:07:00') = DATETIME('1984-12-15 22:15:07')", "t_dt_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testEqCompareDateWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "DATE('2020-09-16') = TIMESTAMP('2020-09-16 00:00:00') AS `d_ts_t`, "
        + "TIMESTAMP('2020-09-16 00:00:00') = DATE('2020-09-16') AS `ts_d_t`, "
        + "DATE('2020-09-16') = TIMESTAMP('1961-04-12 09:07:00') AS `d_ts_f`, "
        + "TIMESTAMP('1984-12-15 09:07:00') = DATE('1984-12-15') AS `ts_d_f`, "
        + "DATE('2020-09-16') = DATETIME('2020-09-16 00:00:00') AS `d_dt_t`, "
        + "DATETIME('2020-09-16 00:00:00') = DATE('2020-09-16') AS `dt_d_t`, "
        + "DATE('1961-04-12') = DATETIME('1984-12-15 22:15:07') AS `d_dt_f`, "
        + "DATETIME('1961-04-12 10:20:30') = DATE('1961-04-12') AS `dt_d_f`, "
        + "DATE('" + today + "') = TIME('00:00:00') AS `d_t_t`, "
        + "TIME('00:00:00') = DATE('" + today + "') AS `t_d_t`, "
        + "DATE('2020-09-16') = TIME('09:07:00') AS `d_t_f`, "
        + "TIME('09:07:00') = DATE('" + today + "') AS `t_d_f` ");
    verifySchema(result,
        schema("DATE('2020-09-16') = TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 00:00:00') = DATE('2020-09-16')", "ts_d_t", "boolean"),
        schema("DATE('2020-09-16') = TIMESTAMP('1961-04-12 09:07:00')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('1984-12-15 09:07:00') = DATE('1984-12-15')", "ts_d_f", "boolean"),
        schema("DATE('2020-09-16') = DATETIME('2020-09-16 00:00:00')", "d_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') = DATE('2020-09-16')", "dt_d_t", "boolean"),
        schema("DATE('1961-04-12') = DATETIME('1984-12-15 22:15:07')", "d_dt_f", "boolean"),
        schema("DATETIME('1961-04-12 10:20:30') = DATE('1961-04-12')", "dt_d_f", "boolean"),
        schema("DATE('" + today + "') = TIME('00:00:00')", "d_t_t", "boolean"),
        schema("TIME('00:00:00') = DATE('" + today + "')", "t_d_t", "boolean"),
        schema("DATE('2020-09-16') = TIME('09:07:00')", "d_t_f", "boolean"),
        schema("TIME('09:07:00') = DATE('" + today + "')", "t_d_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testEqCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIME('10:20:30') = DATETIME('" + today + " 10:20:30') AS `t_dt_t`, "
        + "DATETIME('" + today + " 10:20:30') = TIME('10:20:30') AS `dt_t_t`, "
        + "TIME('09:07:00') = DATETIME('1961-04-12 09:07:00') AS `t_dt_f`, "
        + "DATETIME('" + today + " 09:07:00') = TIME('10:20:30') AS `dt_t_f`, "
        + "TIME('10:20:30') = TIMESTAMP('" + today + " 10:20:30') AS `t_ts_t`, "
        + "TIMESTAMP('" + today + " 10:20:30') = TIME('10:20:30') AS `ts_t_t`, "
        + "TIME('22:15:07') = TIMESTAMP('1984-12-15 22:15:07') AS `t_ts_f`, "
        + "TIMESTAMP('1984-12-15 10:20:30') = TIME('10:20:30') AS `ts_t_f`, "
        + "TIME('00:00:00') = DATE('" + today + "') AS `t_d_t`, "
        + "DATE('" + today + "') = TIME('00:00:00') AS `d_t_t`, "
        + "TIME('09:07:00') = DATE('" + today + "') AS `t_d_f`, "
        + "DATE('2020-09-16') = TIME('09:07:00') AS `d_t_f` ");
    verifySchema(result,
        schema("TIME('10:20:30') = DATETIME('" + today + " 10:20:30')", "t_dt_t", "boolean"),
        schema("DATETIME('" + today + " 10:20:30') = TIME('10:20:30')", "dt_t_t", "boolean"),
        schema("TIME('09:07:00') = DATETIME('1961-04-12 09:07:00')", "t_dt_f", "boolean"),
        schema("DATETIME('" + today + " 09:07:00') = TIME('10:20:30')", "dt_t_f", "boolean"),
        schema("TIME('10:20:30') = TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('" + today + " 10:20:30') = TIME('10:20:30')", "ts_t_t", "boolean"),
        schema("TIME('22:15:07') = TIMESTAMP('1984-12-15 22:15:07')", "t_ts_f", "boolean"),
        schema("TIMESTAMP('1984-12-15 10:20:30') = TIME('10:20:30')", "ts_t_f", "boolean"),
        schema("TIME('00:00:00') = DATE('" + today + "')", "t_d_t", "boolean"),
        schema("DATE('" + today + "') = TIME('00:00:00')", "d_t_t", "boolean"),
        schema("TIME('09:07:00') = DATE('" + today + "')", "t_d_f", "boolean"),
        schema("DATE('2020-09-16') = TIME('09:07:00')", "d_t_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testNeqCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIMESTAMP('2020-09-16 10:20:30') != DATETIME('1961-04-12 09:07:00') AS `ts_dt_t`, "
        + "DATETIME('1961-04-12 09:07:00') != TIMESTAMP('1984-12-15 22:15:07') AS `dt_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') != DATETIME('2020-09-16 10:20:30') AS `ts_dt_f`, "
        + "DATETIME('2020-09-16 10:20:30') != TIMESTAMP('2020-09-16 10:20:30') AS `dt_ts_f`, "
        + "TIMESTAMP('2020-09-16 10:20:30') != DATE('1961-04-12') AS `ts_d_t`, "
        + "DATE('1961-04-12') != TIMESTAMP('1984-12-15 22:15:07') AS `d_ts_t`, "
        + "TIMESTAMP('2020-09-16 00:00:00') != DATE('2020-09-16') AS `ts_d_f`, "
        + "DATE('2020-09-16') != TIMESTAMP('2020-09-16 00:00:00') AS `d_ts_f`, "
        + "TIMESTAMP('2020-09-16 10:20:30') != TIME('09:07:00') AS `ts_t_t`, "
        + "TIME('09:07:00') != TIMESTAMP('1984-12-15 22:15:07') AS `t_ts_t`, "
        + "TIMESTAMP('" + today + " 10:20:30') != TIME('10:20:30') AS `ts_t_f`, "
        + "TIME('10:20:30') != TIMESTAMP('" + today + " 10:20:30') AS `t_ts_f` ");
    verifySchema(result,
        schema("TIMESTAMP('2020-09-16 10:20:30') != DATETIME('1961-04-12 09:07:00')", "ts_dt_t", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') != TIMESTAMP('1984-12-15 22:15:07')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') != DATETIME('2020-09-16 10:20:30')", "ts_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') != TIMESTAMP('2020-09-16 10:20:30')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') != DATE('1961-04-12')", "ts_d_t", "boolean"),
        schema("DATE('1961-04-12') != TIMESTAMP('1984-12-15 22:15:07')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 00:00:00') != DATE('2020-09-16')", "ts_d_f", "boolean"),
        schema("DATE('2020-09-16') != TIMESTAMP('2020-09-16 00:00:00')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') != TIME('09:07:00')", "ts_t_t", "boolean"),
        schema("TIME('09:07:00') != TIMESTAMP('1984-12-15 22:15:07')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('" + today + " 10:20:30') != TIME('10:20:30')", "ts_t_f", "boolean"),
        schema("TIME('10:20:30') != TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testNeqCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "DATETIME('2020-09-16 10:20:30') != TIMESTAMP('1961-04-12 09:07:00') AS `dt_ts_t`, "
        + "TIMESTAMP('1961-04-12 09:07:00') != DATETIME('1984-12-15 22:15:07') AS `ts_dt_t`, "
        + "DATETIME('2020-09-16 10:20:30') != TIMESTAMP('2020-09-16 10:20:30') AS `dt_ts_f`, "
        + "TIMESTAMP('2020-09-16 10:20:30') != DATETIME('2020-09-16 10:20:30') AS `ts_dt_f`, "
        + "DATETIME('2020-09-16 10:20:30') != DATE('1961-04-12') AS `dt_d_t`, "
        + "DATE('1961-04-12') != DATETIME('1984-12-15 22:15:07') AS `d_dt_t`, "
        + "DATETIME('2020-09-16 00:00:00') != DATE('2020-09-16') AS `dt_d_f`, "
        + "DATE('2020-09-16') != DATETIME('2020-09-16 00:00:00') AS `d_dt_f`, "
        + "DATETIME('2020-09-16 10:20:30') != TIME('09:07:00') AS `dt_t_t`, "
        + "TIME('09:07:00') != DATETIME('1984-12-15 22:15:07') AS `t_dt_t`, "
        + "DATETIME('" + today + " 10:20:30') != TIME('10:20:30') AS `dt_t_f`, "
        + "TIME('10:20:30') != DATETIME('" + today + " 10:20:30') AS `t_dt_f` ");
    verifySchema(result,
        schema("DATETIME('2020-09-16 10:20:30') != TIMESTAMP('1961-04-12 09:07:00')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') != DATETIME('1984-12-15 22:15:07')", "ts_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') != TIMESTAMP('2020-09-16 10:20:30')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') != DATETIME('2020-09-16 10:20:30')", "ts_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') != DATE('1961-04-12')", "dt_d_t", "boolean"),
        schema("DATE('1961-04-12') != DATETIME('1984-12-15 22:15:07')", "d_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') != DATE('2020-09-16')", "dt_d_f", "boolean"),
        schema("DATE('2020-09-16') != DATETIME('2020-09-16 00:00:00')", "d_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') != TIME('09:07:00')", "dt_t_t", "boolean"),
        schema("TIME('09:07:00') != DATETIME('1984-12-15 22:15:07')", "t_dt_t", "boolean"),
        schema("DATETIME('" + today + " 10:20:30') != TIME('10:20:30')", "dt_t_f", "boolean"),
        schema("TIME('10:20:30') != DATETIME('" + today + " 10:20:30')", "t_dt_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testNeqCompareDateWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "DATE('2020-09-16') != TIMESTAMP('1961-04-12 09:07:00') AS `d_ts_t`, "
        + "TIMESTAMP('1984-12-15 09:07:00') != DATE('1984-12-15') AS `ts_d_t`, "
        + "DATE('2020-09-16') != TIMESTAMP('2020-09-16 00:00:00') AS `d_ts_f`, "
        + "TIMESTAMP('2020-09-16 00:00:00') != DATE('2020-09-16') AS `ts_d_f`, "
        + "DATE('1961-04-12') != DATETIME('1984-12-15 22:15:07') AS `d_dt_t`, "
        + "DATETIME('1961-04-12 10:20:30') != DATE('1961-04-12') AS `dt_d_t`, "
        + "DATE('2020-09-16') != DATETIME('2020-09-16 00:00:00') AS `d_dt_f`, "
        + "DATETIME('2020-09-16 00:00:00') != DATE('2020-09-16') AS `dt_d_f`, "
        + "DATE('2020-09-16') != TIME('09:07:00') AS `d_t_t`, "
        + "TIME('09:07:00') != DATE('" + today + "') AS `t_d_t`, "
        + "DATE('" + today + "') != TIME('00:00:00') AS `d_t_f`, "
        + "TIME('00:00:00') != DATE('" + today + "') AS `t_d_f` ");
    verifySchema(result,
        schema("DATE('2020-09-16') != TIMESTAMP('1961-04-12 09:07:00')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('1984-12-15 09:07:00') != DATE('1984-12-15')", "ts_d_t", "boolean"),
        schema("DATE('2020-09-16') != TIMESTAMP('2020-09-16 00:00:00')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 00:00:00') != DATE('2020-09-16')", "ts_d_f", "boolean"),
        schema("DATE('1961-04-12') != DATETIME('1984-12-15 22:15:07')", "d_dt_t", "boolean"),
        schema("DATETIME('1961-04-12 10:20:30') != DATE('1961-04-12')", "dt_d_t", "boolean"),
        schema("DATE('2020-09-16') != DATETIME('2020-09-16 00:00:00')", "d_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') != DATE('2020-09-16')", "dt_d_f", "boolean"),
        schema("DATE('2020-09-16') != TIME('09:07:00')", "d_t_t", "boolean"),
        schema("TIME('09:07:00') != DATE('" + today + "')", "t_d_t", "boolean"),
        schema("DATE('" + today + "') != TIME('00:00:00')", "d_t_f", "boolean"),
        schema("TIME('00:00:00') != DATE('" + today + "')", "t_d_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testNeqCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIME('09:07:00') != DATETIME('1961-04-12 09:07:00') AS `t_dt_t`, "
        + "DATETIME('" + today + " 09:07:00') != TIME('10:20:30') AS `dt_t_t`, "
        + "TIME('10:20:30') != DATETIME('" + today + " 10:20:30') AS `t_dt_f`, "
        + "DATETIME('" + today + " 10:20:30') != TIME('10:20:30') AS `dt_t_f`, "
        + "TIME('22:15:07') != TIMESTAMP('1984-12-15 22:15:07') AS `t_ts_t`, "
        + "TIMESTAMP('1984-12-15 10:20:30') != TIME('10:20:30') AS `ts_t_t`, "
        + "TIME('10:20:30') != TIMESTAMP('" + today + " 10:20:30') AS `t_ts_f`, "
        + "TIMESTAMP('" + today + " 10:20:30') != TIME('10:20:30') AS `ts_t_f`, "
        + "TIME('09:07:00') != DATE('" + today + "') AS `t_d_t`, "
        + "DATE('2020-09-16') != TIME('09:07:00') AS `d_t_t`, "
        + "TIME('00:00:00') != DATE('" + today + "') AS `t_d_f`, "
        + "DATE('" + today + "') != TIME('00:00:00') AS `d_t_f` ");
    verifySchema(result,
        schema("TIME('09:07:00') != DATETIME('1961-04-12 09:07:00')", "t_dt_t", "boolean"),
        schema("DATETIME('" + today + " 09:07:00') != TIME('10:20:30')", "dt_t_t", "boolean"),
        schema("TIME('10:20:30') != DATETIME('" + today + " 10:20:30')", "t_dt_f", "boolean"),
        schema("DATETIME('" + today + " 10:20:30') != TIME('10:20:30')", "dt_t_f", "boolean"),
        schema("TIME('22:15:07') != TIMESTAMP('1984-12-15 22:15:07')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('1984-12-15 10:20:30') != TIME('10:20:30')", "ts_t_t", "boolean"),
        schema("TIME('10:20:30') != TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", "boolean"),
        schema("TIMESTAMP('" + today + " 10:20:30') != TIME('10:20:30')", "ts_t_f", "boolean"),
        schema("TIME('09:07:00') != DATE('" + today + "')", "t_d_t", "boolean"),
        schema("DATE('2020-09-16') != TIME('09:07:00')", "d_t_t", "boolean"),
        schema("TIME('00:00:00') != DATE('" + today + "')", "t_d_f", "boolean"),
        schema("DATE('" + today + "') != TIME('00:00:00')", "d_t_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLtCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIMESTAMP('2020-09-16 10:20:30') < DATETIME('2061-04-12 09:07:00') AS `ts_dt_t`, "
        + "DATETIME('1961-04-12 09:07:00') < TIMESTAMP('1984-12-15 22:15:07') AS `dt_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') < DATETIME('2020-09-16 10:20:30') AS `ts_dt_f`, "
        + "DATETIME('2020-09-16 10:20:30') < TIMESTAMP('1961-04-12 09:07:00') AS `dt_ts_f`, "
        + "TIMESTAMP('2020-09-16 10:20:30') < DATE('2077-04-12') AS `ts_d_t`, "
        + "DATE('1961-04-12') < TIMESTAMP('1984-12-15 22:15:07') AS `d_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') < DATE('1961-04-12') AS `ts_d_f`, "
        + "DATE('2020-09-16') < TIMESTAMP('2020-09-16 00:00:00') AS `d_ts_f`, "
        + "TIMESTAMP('2020-09-16 10:20:30') < TIME('09:07:00') AS `ts_t_t`, "
        + "TIME('09:07:00') < TIMESTAMP('3077-12-15 22:15:07') AS `t_ts_t`, "
        + "TIMESTAMP('" + today + " 10:20:30') < TIME('10:20:30') AS `ts_t_f`, "
        + "TIME('20:50:40') < TIMESTAMP('" + today + " 10:20:30') AS `t_ts_f` ");
    verifySchema(result,
        schema("TIMESTAMP('2020-09-16 10:20:30') < DATETIME('2061-04-12 09:07:00')", "ts_dt_t", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') < TIMESTAMP('1984-12-15 22:15:07')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') < DATETIME('2020-09-16 10:20:30')", "ts_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') < TIMESTAMP('1961-04-12 09:07:00')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') < DATE('2077-04-12')", "ts_d_t", "boolean"),
        schema("DATE('1961-04-12') < TIMESTAMP('1984-12-15 22:15:07')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') < DATE('1961-04-12')", "ts_d_f", "boolean"),
        schema("DATE('2020-09-16') < TIMESTAMP('2020-09-16 00:00:00')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') < TIME('09:07:00')", "ts_t_t", "boolean"),
        schema("TIME('09:07:00') < TIMESTAMP('3077-12-15 22:15:07')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('" + today + " 10:20:30') < TIME('10:20:30')", "ts_t_f", "boolean"),
        schema("TIME('20:50:40') < TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLtCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "DATETIME('2020-09-16 10:20:30') < TIMESTAMP('2077-04-12 09:07:00') AS `dt_ts_t`, "
        + "TIMESTAMP('1961-04-12 09:07:00') < DATETIME('1984-12-15 22:15:07') AS `ts_dt_t`, "
        + "DATETIME('2020-09-16 10:20:30') < TIMESTAMP('2020-09-16 10:20:30') AS `dt_ts_f`, "
        + "TIMESTAMP('2020-09-16 10:20:30') < DATETIME('1984-12-15 22:15:07') AS `ts_dt_f`, "
        + "DATETIME('2020-09-16 10:20:30') < DATE('3077-04-12') AS `dt_d_t`, "
        + "DATE('1961-04-12') < DATETIME('1984-12-15 22:15:07') AS `d_dt_t`, "
        + "DATETIME('2020-09-16 00:00:00') < DATE('2020-09-16') AS `dt_d_f`, "
        + "DATE('2020-09-16') < DATETIME('1961-04-12 09:07:00') AS `d_dt_f`, "
        + "DATETIME('2020-09-16 10:20:30') < TIME('09:07:00') AS `dt_t_t`, "
        + "TIME('09:07:00') < DATETIME('3077-12-15 22:15:07') AS `t_dt_t`, "
        + "DATETIME('" + today + " 10:20:30') < TIME('10:20:30') AS `dt_t_f`, "
        + "TIME('20:40:50') < DATETIME('" + today + " 10:20:30') AS `t_dt_f` ");
    verifySchema(result,
        schema("DATETIME('2020-09-16 10:20:30') < TIMESTAMP('2077-04-12 09:07:00')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') < DATETIME('1984-12-15 22:15:07')", "ts_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') < TIMESTAMP('2020-09-16 10:20:30')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') < DATETIME('1984-12-15 22:15:07')", "ts_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') < DATE('3077-04-12')", "dt_d_t", "boolean"),
        schema("DATE('1961-04-12') < DATETIME('1984-12-15 22:15:07')", "d_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') < DATE('2020-09-16')", "dt_d_f", "boolean"),
        schema("DATE('2020-09-16') < DATETIME('1961-04-12 09:07:00')", "d_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') < TIME('09:07:00')", "dt_t_t", "boolean"),
        schema("TIME('09:07:00') < DATETIME('3077-12-15 22:15:07')", "t_dt_t", "boolean"),
        schema("DATETIME('" + today + " 10:20:30') < TIME('10:20:30')", "dt_t_f", "boolean"),
        schema("TIME('20:40:50') < DATETIME('" + today + " 10:20:30')", "t_dt_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLtCompareDateWithOtherTypes() throws IOException {
    JSONObject result = executeQuery("select "
        + "DATE('2020-09-16') < TIMESTAMP('3077-04-12 09:07:00') AS `d_ts_t`, "
        + "TIMESTAMP('1961-04-12 09:07:00') < DATE('1984-12-15') AS `ts_d_t`, "
        + "DATE('2020-09-16') < TIMESTAMP('2020-09-16 00:00:00') AS `d_ts_f`, "
        + "TIMESTAMP('2077-04-12 09:07:00') < DATE('2020-09-16') AS `ts_d_f`, "
        + "DATE('1961-04-12') < DATETIME('1984-12-15 22:15:07') AS `d_dt_t`, "
        + "DATETIME('1961-04-12 10:20:30') < DATE('1984-11-15') AS `dt_d_t`, "
        + "DATE('2020-09-16') < DATETIME('2020-09-16 00:00:00') AS `d_dt_f`, "
        + "DATETIME('2020-09-16 00:00:00') < DATE('1984-03-22') AS `dt_d_f`, "
        + "DATE('2020-09-16') < TIME('09:07:00') AS `d_t_t`, "
        + "TIME('09:07:00') < DATE('3077-04-12') AS `t_d_t`, "
        + "DATE('3077-04-12') < TIME('00:00:00') AS `d_t_f`, "
        + "TIME('00:00:00') < DATE('2020-09-16') AS `t_d_f` ");
    verifySchema(result,
        schema("DATE('2020-09-16') < TIMESTAMP('3077-04-12 09:07:00')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') < DATE('1984-12-15')", "ts_d_t", "boolean"),
        schema("DATE('2020-09-16') < TIMESTAMP('2020-09-16 00:00:00')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('2077-04-12 09:07:00') < DATE('2020-09-16')", "ts_d_f", "boolean"),
        schema("DATE('1961-04-12') < DATETIME('1984-12-15 22:15:07')", "d_dt_t", "boolean"),
        schema("DATETIME('1961-04-12 10:20:30') < DATE('1984-11-15')", "dt_d_t", "boolean"),
        schema("DATE('2020-09-16') < DATETIME('2020-09-16 00:00:00')", "d_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') < DATE('1984-03-22')", "dt_d_f", "boolean"),
        schema("DATE('2020-09-16') < TIME('09:07:00')", "d_t_t", "boolean"),
        schema("TIME('09:07:00') < DATE('3077-04-12')", "t_d_t", "boolean"),
        schema("DATE('3077-04-12') < TIME('00:00:00')", "d_t_f", "boolean"),
        schema("TIME('00:00:00') < DATE('2020-09-16')", "t_d_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLtCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIME('09:07:00') < DATETIME('3077-04-12 09:07:00') AS `t_dt_t`, "
        + "DATETIME('" + today + " 09:07:00') < TIME('10:20:30') AS `dt_t_t`, "
        + "TIME('10:20:30') < DATETIME('" + today + " 10:20:30') AS `t_dt_f`, "
        + "DATETIME('" + today + " 20:40:50') < TIME('10:20:30') AS `dt_t_f`, "
        + "TIME('22:15:07') < TIMESTAMP('3077-12-15 22:15:07') AS `t_ts_t`, "
        + "TIMESTAMP('1984-12-15 10:20:30') < TIME('10:20:30') AS `ts_t_t`, "
        + "TIME('10:20:30') < TIMESTAMP('" + today + " 10:20:30') AS `t_ts_f`, "
        + "TIMESTAMP('" + today + " 20:50:42') < TIME('10:20:30') AS `ts_t_f`, "
        + "TIME('09:07:00') < DATE('3077-04-12') AS `t_d_t`, "
        + "DATE('2020-09-16') < TIME('09:07:00') AS `d_t_t`, "
        + "TIME('00:00:00') < DATE('1961-04-12') AS `t_d_f`, "
        + "DATE('3077-04-12') < TIME('10:20:30') AS `d_t_f` ");
    verifySchema(result,
        schema("TIME('09:07:00') < DATETIME('3077-04-12 09:07:00')", "t_dt_t", "boolean"),
        schema("DATETIME('" + today + " 09:07:00') < TIME('10:20:30')", "dt_t_t", "boolean"),
        schema("TIME('10:20:30') < DATETIME('" + today + " 10:20:30')", "t_dt_f", "boolean"),
        schema("DATETIME('" + today + " 20:40:50') < TIME('10:20:30')", "dt_t_f", "boolean"),
        schema("TIME('22:15:07') < TIMESTAMP('3077-12-15 22:15:07')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('1984-12-15 10:20:30') < TIME('10:20:30')", "ts_t_t", "boolean"),
        schema("TIME('10:20:30') < TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", "boolean"),
        schema("TIMESTAMP('" + today + " 20:50:42') < TIME('10:20:30')", "ts_t_f", "boolean"),
        schema("TIME('09:07:00') < DATE('3077-04-12')", "t_d_t", "boolean"),
        schema("DATE('2020-09-16') < TIME('09:07:00')", "d_t_t", "boolean"),
        schema("TIME('00:00:00') < DATE('1961-04-12')", "t_d_f", "boolean"),
        schema("DATE('3077-04-12') < TIME('10:20:30')", "d_t_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGtCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIMESTAMP('2020-09-16 10:20:30') > DATETIME('2020-09-16 10:20:25') AS `ts_dt_t`, "
        + "DATETIME('2020-09-16 10:20:30') > TIMESTAMP('1961-04-12 09:07:00') AS `dt_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') > DATETIME('2061-04-12 09:07:00') AS `ts_dt_f`, "
        + "DATETIME('1961-04-12 09:07:00') > TIMESTAMP('1984-12-15 09:07:00') AS `dt_ts_f`, "
        + "TIMESTAMP('2020-09-16 10:20:30') > DATE('1961-04-12') AS `ts_d_t`, "
        + "DATE('2020-09-16') > TIMESTAMP('2020-09-15 22:15:07') AS `d_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') > DATE('2077-04-12') AS `ts_d_f`, "
        + "DATE('1961-04-12') > TIMESTAMP('1961-04-12 00:00:00') AS `d_ts_f`, "
        + "TIMESTAMP('3077-07-08 20:20:30') > TIME('10:20:30') AS `ts_t_t`, "
        + "TIME('20:50:40') > TIMESTAMP('" + today + " 10:20:30') AS `t_ts_t`, "
        + "TIMESTAMP('" + today + " 10:20:30') > TIME('10:20:30') AS `ts_t_f`, "
        + "TIME('09:07:00') > TIMESTAMP('3077-12-15 22:15:07') AS `t_ts_f` ");
    verifySchema(result,
        schema("TIMESTAMP('2020-09-16 10:20:30') > DATETIME('2020-09-16 10:20:25')", "ts_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') > TIMESTAMP('1961-04-12 09:07:00')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') > DATETIME('2061-04-12 09:07:00')", "ts_dt_f", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') > TIMESTAMP('1984-12-15 09:07:00')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') > DATE('1961-04-12')", "ts_d_t", "boolean"),
        schema("DATE('2020-09-16') > TIMESTAMP('2020-09-15 22:15:07')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') > DATE('2077-04-12')", "ts_d_f", "boolean"),
        schema("DATE('1961-04-12') > TIMESTAMP('1961-04-12 00:00:00')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('3077-07-08 20:20:30') > TIME('10:20:30')", "ts_t_t", "boolean"),
        schema("TIME('20:50:40') > TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('" + today + " 10:20:30') > TIME('10:20:30')", "ts_t_f", "boolean"),
        schema("TIME('09:07:00') > TIMESTAMP('3077-12-15 22:15:07')", "t_ts_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGtCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "DATETIME('2020-09-16 10:20:31') > TIMESTAMP('2020-09-16 10:20:30') AS `dt_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') > DATETIME('1984-12-15 22:15:07') AS `ts_dt_t`, "
        + "DATETIME('2020-09-16 10:20:30') > TIMESTAMP('2077-04-12 09:07:00') AS `dt_ts_f`, "
        + "TIMESTAMP('1961-04-12 09:07:00') > DATETIME('1961-04-12 09:07:00') AS `ts_dt_f`, "
        + "DATETIME('3077-04-12 10:20:30') > DATE('2020-09-16') AS `dt_d_t`, "
        + "DATE('2020-09-16') > DATETIME('1961-04-12 09:07:00') AS `d_dt_t`, "
        + "DATETIME('2020-09-16 00:00:00') > DATE('2020-09-16') AS `dt_d_f`, "
        + "DATE('1961-04-12') > DATETIME('1984-12-15 22:15:07') AS `d_dt_f`, "
        + "DATETIME('3077-04-12 10:20:30') > TIME('09:07:00') AS `dt_t_t`, "
        + "TIME('20:40:50') > DATETIME('" + today + " 10:20:30') AS `t_dt_t`, "
        + "DATETIME('" + today + " 10:20:30') > TIME('10:20:30') AS `dt_t_f`, "
        + "TIME('09:07:00') > DATETIME('3077-12-15 22:15:07') AS `t_dt_f` ");
    verifySchema(result,
        schema("DATETIME('2020-09-16 10:20:31') > TIMESTAMP('2020-09-16 10:20:30')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') > DATETIME('1984-12-15 22:15:07')", "ts_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') > TIMESTAMP('2077-04-12 09:07:00')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') > DATETIME('1961-04-12 09:07:00')", "ts_dt_f", "boolean"),
        schema("DATETIME('3077-04-12 10:20:30') > DATE('2020-09-16')", "dt_d_t", "boolean"),
        schema("DATE('2020-09-16') > DATETIME('1961-04-12 09:07:00')", "d_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') > DATE('2020-09-16')", "dt_d_f", "boolean"),
        schema("DATE('1961-04-12') > DATETIME('1984-12-15 22:15:07')", "d_dt_f", "boolean"),
        schema("DATETIME('3077-04-12 10:20:30') > TIME('09:07:00')", "dt_t_t", "boolean"),
        schema("TIME('20:40:50') > DATETIME('" + today + " 10:20:30')", "t_dt_t", "boolean"),
        schema("DATETIME('" + today + " 10:20:30') > TIME('10:20:30')", "dt_t_f", "boolean"),
        schema("TIME('09:07:00') > DATETIME('3077-12-15 22:15:07')", "t_dt_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGtCompareDateWithOtherTypes() throws IOException {
    JSONObject result = executeQuery("select "
        + "DATE('2020-09-16') > TIMESTAMP('1961-04-12 09:07:00') AS `d_ts_t`, "
        + "TIMESTAMP('2077-04-12 09:07:00') > DATE('2020-09-16') AS `ts_d_t`, "
        + "DATE('2020-09-16') > TIMESTAMP('2020-09-16 00:00:00') AS `d_ts_f`, "
        + "TIMESTAMP('1961-04-12 09:07:00') > DATE('1984-12-15') AS `ts_d_f`, "
        + "DATE('1984-12-15') > DATETIME('1961-04-12 09:07:00') AS `d_dt_t`, "
        + "DATETIME('2020-09-16 00:00:00') > DATE('1984-03-22') AS `dt_d_t`, "
        + "DATE('2020-09-16') > DATETIME('2020-09-16 00:00:00') AS `d_dt_f`, "
        + "DATETIME('1961-04-12 10:20:30') > DATE('1984-11-15') AS `dt_d_f`, "
        + "DATE('3077-04-12') > TIME('00:00:00') AS `d_t_t`, "
        + "TIME('00:00:00') > DATE('2020-09-16') AS `t_d_t`, "
        + "DATE('2020-09-16') > TIME('09:07:00') AS `d_t_f`, "
        + "TIME('09:07:00') > DATE('3077-04-12') AS `t_d_f` ");
    verifySchema(result,
        schema("DATE('2020-09-16') > TIMESTAMP('1961-04-12 09:07:00')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('2077-04-12 09:07:00') > DATE('2020-09-16')", "ts_d_t", "boolean"),
        schema("DATE('2020-09-16') > TIMESTAMP('2020-09-16 00:00:00')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') > DATE('1984-12-15')", "ts_d_f", "boolean"),
        schema("DATE('1984-12-15') > DATETIME('1961-04-12 09:07:00')", "d_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') > DATE('1984-03-22')", "dt_d_t", "boolean"),
        schema("DATE('2020-09-16') > DATETIME('2020-09-16 00:00:00')", "d_dt_f", "boolean"),
        schema("DATETIME('1961-04-12 10:20:30') > DATE('1984-11-15')", "dt_d_f", "boolean"),
        schema("DATE('3077-04-12') > TIME('00:00:00')", "d_t_t", "boolean"),
        schema("TIME('00:00:00') > DATE('2020-09-16')", "t_d_t", "boolean"),
        schema("DATE('2020-09-16') > TIME('09:07:00')", "d_t_f", "boolean"),
        schema("TIME('09:07:00') > DATE('3077-04-12')", "t_d_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGtCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIME('09:07:00') > DATETIME('1961-04-12 09:07:00') AS `t_dt_t`, "
        + "DATETIME('" + today + " 20:40:50') > TIME('10:20:30') AS `dt_t_t`, "
        + "TIME('10:20:30') > DATETIME('" + today + " 10:20:30') AS `t_dt_f`, "
        + "DATETIME('" + today + " 09:07:00') > TIME('10:20:30') AS `dt_t_f`, "
        + "TIME('22:15:07') > TIMESTAMP('1984-12-15 22:15:07') AS `t_ts_t`, "
        + "TIMESTAMP('" + today + " 20:50:42') > TIME('10:20:30') AS `ts_t_t`, "
        + "TIME('10:20:30') > TIMESTAMP('" + today + " 10:20:30') AS `t_ts_f`, "
        + "TIMESTAMP('1984-12-15 10:20:30') > TIME('10:20:30') AS `ts_t_f`, "
        + "TIME('00:00:00') > DATE('1961-04-12') AS `t_d_t`, "
        + "DATE('3077-04-12') > TIME('10:20:30') AS `d_t_t`, "
        + "TIME('09:07:00') > DATE('3077-04-12') AS `t_d_f`, "
        + "DATE('2020-09-16') > TIME('09:07:00') AS `d_t_f` ");
    verifySchema(result,
        schema("TIME('09:07:00') > DATETIME('1961-04-12 09:07:00')", "t_dt_t", "boolean"),
        schema("DATETIME('" + today + " 20:40:50') > TIME('10:20:30')", "dt_t_t", "boolean"),
        schema("TIME('10:20:30') > DATETIME('" + today + " 10:20:30')", "t_dt_f", "boolean"),
        schema("DATETIME('" + today + " 09:07:00') > TIME('10:20:30')", "dt_t_f", "boolean"),
        schema("TIME('22:15:07') > TIMESTAMP('1984-12-15 22:15:07')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('" + today + " 20:50:42') > TIME('10:20:30')", "ts_t_t", "boolean"),
        schema("TIME('10:20:30') > TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", "boolean"),
        schema("TIMESTAMP('1984-12-15 10:20:30') > TIME('10:20:30')", "ts_t_f", "boolean"),
        schema("TIME('00:00:00') > DATE('1961-04-12')", "t_d_t", "boolean"),
        schema("DATE('3077-04-12') > TIME('10:20:30')", "d_t_t", "boolean"),
        schema("TIME('09:07:00') > DATE('3077-04-12')", "t_d_f", "boolean"),
        schema("DATE('2020-09-16') > TIME('09:07:00')", "d_t_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLteCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIMESTAMP('2020-09-16 10:20:30') <= DATETIME('2020-09-16 10:20:30') AS `ts_dt_t`, "
        + "DATETIME('1961-04-12 09:07:00') <= TIMESTAMP('1984-12-15 22:15:07') AS `dt_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') <= DATETIME('1961-04-12 09:07:00') AS `ts_dt_f`, "
        + "DATETIME('2020-09-16 10:20:30') <= TIMESTAMP('1961-04-12 09:07:00') AS `dt_ts_f`, "
        + "TIMESTAMP('2020-09-16 10:20:30') <= DATE('2077-04-12') AS `ts_d_t`, "
        + "DATE('2020-09-16') <= TIMESTAMP('2020-09-16 00:00:00') AS `d_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') <= DATE('1961-04-12') AS `ts_d_f`, "
        + "DATE('2077-04-12') <= TIMESTAMP('1984-12-15 22:15:07') AS `d_ts_f`, "
        + "TIMESTAMP('" + today + " 10:20:30') <= TIME('10:20:30') AS `ts_t_t`, "
        + "TIME('09:07:00') <= TIMESTAMP('3077-12-15 22:15:07') AS `t_ts_t`, "
        + "TIMESTAMP('3077-09-16 10:20:30') <= TIME('09:07:00') AS `ts_t_f`, "
        + "TIME('20:50:40') <= TIMESTAMP('" + today + " 10:20:30') AS `t_ts_f` ");
    verifySchema(result,
        schema("TIMESTAMP('2020-09-16 10:20:30') <= DATETIME('2020-09-16 10:20:30')", "ts_dt_t", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') <= TIMESTAMP('1984-12-15 22:15:07')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') <= DATETIME('1961-04-12 09:07:00')", "ts_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') <= TIMESTAMP('1961-04-12 09:07:00')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') <= DATE('2077-04-12')", "ts_d_t", "boolean"),
        schema("DATE('2020-09-16') <= TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') <= DATE('1961-04-12')", "ts_d_f", "boolean"),
        schema("DATE('2077-04-12') <= TIMESTAMP('1984-12-15 22:15:07')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('" + today + " 10:20:30') <= TIME('10:20:30')", "ts_t_t", "boolean"),
        schema("TIME('09:07:00') <= TIMESTAMP('3077-12-15 22:15:07')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('3077-09-16 10:20:30') <= TIME('09:07:00')", "ts_t_f", "boolean"),
        schema("TIME('20:50:40') <= TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLteCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "DATETIME('2020-09-16 10:20:30') <= TIMESTAMP('2020-09-16 10:20:30') AS `dt_ts_t`, "
        + "TIMESTAMP('1961-04-12 09:07:00') <= DATETIME('1984-12-15 22:15:07') AS `ts_dt_t`, "
        + "DATETIME('3077-09-16 10:20:30') <= TIMESTAMP('2077-04-12 09:07:00') AS `dt_ts_f`, "
        + "TIMESTAMP('2020-09-16 10:20:30') <= DATETIME('1984-12-15 22:15:07') AS `ts_dt_f`, "
        + "DATETIME('2020-09-16 00:00:00') <= DATE('2020-09-16') AS `dt_d_t`, "
        + "DATE('1961-04-12') <= DATETIME('1984-12-15 22:15:07') AS `d_dt_t`, "
        + "DATETIME('2020-09-16 10:20:30') <= DATE('1984-04-12') AS `dt_d_f`, "
        + "DATE('2020-09-16') <= DATETIME('1961-04-12 09:07:00') AS `d_dt_f`, "
        + "DATETIME('" + today + " 10:20:30') <= TIME('10:20:30') AS `dt_t_t`, "
        + "TIME('09:07:00') <= DATETIME('3077-12-15 22:15:07') AS `t_dt_t`, "
        + "DATETIME('3077-09-16 10:20:30') <= TIME('19:07:00') AS `dt_t_f`, "
        + "TIME('20:40:50') <= DATETIME('" + today + " 10:20:30') AS `t_dt_f` ");
    verifySchema(result,
        schema("DATETIME('2020-09-16 10:20:30') <= TIMESTAMP('2020-09-16 10:20:30')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') <= DATETIME('1984-12-15 22:15:07')", "ts_dt_t", "boolean"),
        schema("DATETIME('3077-09-16 10:20:30') <= TIMESTAMP('2077-04-12 09:07:00')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') <= DATETIME('1984-12-15 22:15:07')", "ts_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') <= DATE('2020-09-16')", "dt_d_t", "boolean"),
        schema("DATE('1961-04-12') <= DATETIME('1984-12-15 22:15:07')", "d_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') <= DATE('1984-04-12')", "dt_d_f", "boolean"),
        schema("DATE('2020-09-16') <= DATETIME('1961-04-12 09:07:00')", "d_dt_f", "boolean"),
        schema("DATETIME('" + today + " 10:20:30') <= TIME('10:20:30')", "dt_t_t", "boolean"),
        schema("TIME('09:07:00') <= DATETIME('3077-12-15 22:15:07')", "t_dt_t", "boolean"),
        schema("DATETIME('3077-09-16 10:20:30') <= TIME('19:07:00')", "dt_t_f", "boolean"),
        schema("TIME('20:40:50') <= DATETIME('" + today + " 10:20:30')", "t_dt_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLteCompareDateWithOtherTypes() throws IOException {
    JSONObject result = executeQuery("select "
        + "DATE('2020-09-16') <= TIMESTAMP('2020-09-16 00:00:00') AS `d_ts_t`, "
        + "TIMESTAMP('1961-04-12 09:07:00') <= DATE('1984-12-15') AS `ts_d_t`, "
        + "DATE('2020-09-16') <= TIMESTAMP('1961-04-12 09:07:00') AS `d_ts_f`, "
        + "TIMESTAMP('2077-04-12 09:07:00') <= DATE('2020-09-16') AS `ts_d_f`, "
        + "DATE('2020-09-16') <= DATETIME('2020-09-16 00:00:00') AS `d_dt_t`, "
        + "DATETIME('1961-04-12 10:20:30') <= DATE('1984-11-15') AS `dt_d_t`, "
        + "DATE('2077-04-12') <= DATETIME('1984-12-15 22:15:07') AS `d_dt_f`, "
        + "DATETIME('2020-09-16 00:00:00') <= DATE('1984-03-22') AS `dt_d_f`, "
        + "DATE('2020-09-16') <= TIME('09:07:00') AS `d_t_t`, "
        + "TIME('09:07:00') <= DATE('3077-04-12') AS `t_d_t`, "
        + "DATE('3077-04-12') <= TIME('00:00:00') AS `d_t_f`, "
        + "TIME('00:00:00') <= DATE('2020-09-16') AS `t_d_f` ");
    verifySchema(result,
        schema("DATE('2020-09-16') <= TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') <= DATE('1984-12-15')", "ts_d_t", "boolean"),
        schema("DATE('2020-09-16') <= TIMESTAMP('1961-04-12 09:07:00')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('2077-04-12 09:07:00') <= DATE('2020-09-16')", "ts_d_f", "boolean"),
        schema("DATE('2020-09-16') <= DATETIME('2020-09-16 00:00:00')", "d_dt_t", "boolean"),
        schema("DATETIME('1961-04-12 10:20:30') <= DATE('1984-11-15')", "dt_d_t", "boolean"),
        schema("DATE('2077-04-12') <= DATETIME('1984-12-15 22:15:07')", "d_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') <= DATE('1984-03-22')", "dt_d_f", "boolean"),
        schema("DATE('2020-09-16') <= TIME('09:07:00')", "d_t_t", "boolean"),
        schema("TIME('09:07:00') <= DATE('3077-04-12')", "t_d_t", "boolean"),
        schema("DATE('3077-04-12') <= TIME('00:00:00')", "d_t_f", "boolean"),
        schema("TIME('00:00:00') <= DATE('2020-09-16')", "t_d_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLteCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIME('10:20:30') <= DATETIME('" + today + " 10:20:30') AS `t_dt_t`, "
        + "DATETIME('" + today + " 09:07:00') <= TIME('10:20:30') AS `dt_t_t`, "
        + "TIME('09:07:00') <= DATETIME('1961-04-12 09:07:00') AS `t_dt_f`, "
        + "DATETIME('" + today + " 20:40:50') <= TIME('10:20:30') AS `dt_t_f`, "
        + "TIME('10:20:30') <= TIMESTAMP('" + today + " 10:20:30') AS `t_ts_t`, "
        + "TIMESTAMP('1984-12-15 10:20:30') <= TIME('10:20:30') AS `ts_t_t`, "
        + "TIME('22:15:07') <= TIMESTAMP('1984-12-15 22:15:07') AS `t_ts_f`, "
        + "TIMESTAMP('" + today + " 20:50:42') <= TIME('10:20:30') AS `ts_t_f`, "
        + "TIME('09:07:00') <= DATE('3077-04-12') AS `t_d_t`, "
        + "DATE('2020-09-16') <= TIME('09:07:00') AS `d_t_t`, "
        + "TIME('00:00:00') <= DATE('1961-04-12') AS `t_d_f`, "
        + "DATE('3077-04-12') <= TIME('10:20:30') AS `d_t_f` ");
    verifySchema(result,
        schema("TIME('10:20:30') <= DATETIME('" + today + " 10:20:30')", "t_dt_t", "boolean"),
        schema("DATETIME('" + today + " 09:07:00') <= TIME('10:20:30')", "dt_t_t", "boolean"),
        schema("TIME('09:07:00') <= DATETIME('1961-04-12 09:07:00')", "t_dt_f", "boolean"),
        schema("DATETIME('" + today + " 20:40:50') <= TIME('10:20:30')", "dt_t_f", "boolean"),
        schema("TIME('10:20:30') <= TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('1984-12-15 10:20:30') <= TIME('10:20:30')", "ts_t_t", "boolean"),
        schema("TIME('22:15:07') <= TIMESTAMP('1984-12-15 22:15:07')", "t_ts_f", "boolean"),
        schema("TIMESTAMP('" + today + " 20:50:42') <= TIME('10:20:30')", "ts_t_f", "boolean"),
        schema("TIME('09:07:00') <= DATE('3077-04-12')", "t_d_t", "boolean"),
        schema("DATE('2020-09-16') <= TIME('09:07:00')", "d_t_t", "boolean"),
        schema("TIME('00:00:00') <= DATE('1961-04-12')", "t_d_f", "boolean"),
        schema("DATE('3077-04-12') <= TIME('10:20:30')", "d_t_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGteCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIMESTAMP('2020-09-16 10:20:30') >= DATETIME('2020-09-16 10:20:30') AS `ts_dt_t`, "
        + "DATETIME('2020-09-16 10:20:30') >= TIMESTAMP('1961-04-12 09:07:00') AS `dt_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') >= DATETIME('2061-04-12 09:07:00') AS `ts_dt_f`, "
        + "DATETIME('1961-04-12 09:07:00') >= TIMESTAMP('1984-12-15 09:07:00') AS `dt_ts_f`, "
        + "TIMESTAMP('2020-09-16 10:20:30') >= DATE('1961-04-12') AS `ts_d_t`, "
        + "DATE('2020-09-16') >= TIMESTAMP('2020-09-16 00:00:00') AS `d_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') >= DATE('2077-04-12') AS `ts_d_f`, "
        + "DATE('1961-04-11') >= TIMESTAMP('1961-04-12 00:00:00') AS `d_ts_f`, "
        + "TIMESTAMP('" + today + " 10:20:30') >= TIME('10:20:30') AS `ts_t_t`, "
        + "TIME('20:50:40') >= TIMESTAMP('" + today + " 10:20:30') AS `t_ts_t`, "
        + "TIMESTAMP('1977-07-08 10:20:30') >= TIME('10:20:30') AS `ts_t_f`, "
        + "TIME('09:07:00') >= TIMESTAMP('3077-12-15 22:15:07') AS `t_ts_f` ");
    verifySchema(result,
        schema("TIMESTAMP('2020-09-16 10:20:30') >= DATETIME('2020-09-16 10:20:30')", "ts_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') >= TIMESTAMP('1961-04-12 09:07:00')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') >= DATETIME('2061-04-12 09:07:00')", "ts_dt_f", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') >= TIMESTAMP('1984-12-15 09:07:00')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') >= DATE('1961-04-12')", "ts_d_t", "boolean"),
        schema("DATE('2020-09-16') >= TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') >= DATE('2077-04-12')", "ts_d_f", "boolean"),
        schema("DATE('1961-04-11') >= TIMESTAMP('1961-04-12 00:00:00')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('" + today + " 10:20:30') >= TIME('10:20:30')", "ts_t_t", "boolean"),
        schema("TIME('20:50:40') >= TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('1977-07-08 10:20:30') >= TIME('10:20:30')", "ts_t_f", "boolean"),
        schema("TIME('09:07:00') >= TIMESTAMP('3077-12-15 22:15:07')", "t_ts_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGteCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "DATETIME('2020-09-16 10:20:30') >= TIMESTAMP('2020-09-16 10:20:30') AS `dt_ts_t`, "
        + "TIMESTAMP('2020-09-16 10:20:30') >= DATETIME('1984-12-15 22:15:07') AS `ts_dt_t`, "
        + "DATETIME('2020-09-16 10:20:30') >= TIMESTAMP('2077-04-12 09:07:00') AS `dt_ts_f`, "
        + "TIMESTAMP('1961-04-12 00:00:00') >= DATETIME('1961-04-12 09:07:00') AS `ts_dt_f`, "
        + "DATETIME('2020-09-16 00:00:00') >= DATE('2020-09-16') AS `dt_d_t`, "
        + "DATE('2020-09-16') >= DATETIME('1961-04-12 09:07:00') AS `d_dt_t`, "
        + "DATETIME('1961-04-12 09:07:00') >= DATE('2020-09-16') AS `dt_d_f`, "
        + "DATE('1961-04-12') >= DATETIME('1984-12-15 22:15:07') AS `d_dt_f`, "
        + "DATETIME('" + today + " 10:20:30') >= TIME('10:20:30') AS `dt_t_t`, "
        + "TIME('20:40:50') >= DATETIME('" + today + " 10:20:30') AS `t_dt_t`, "
        + "DATETIME('1961-04-12 09:07:00') >= TIME('09:07:00') AS `dt_t_f`, "
        + "TIME('09:07:00') >= DATETIME('3077-12-15 22:15:07') AS `t_dt_f` ");
    verifySchema(result,
        schema("DATETIME('2020-09-16 10:20:30') >= TIMESTAMP('2020-09-16 10:20:30')", "dt_ts_t", "boolean"),
        schema("TIMESTAMP('2020-09-16 10:20:30') >= DATETIME('1984-12-15 22:15:07')", "ts_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 10:20:30') >= TIMESTAMP('2077-04-12 09:07:00')", "dt_ts_f", "boolean"),
        schema("TIMESTAMP('1961-04-12 00:00:00') >= DATETIME('1961-04-12 09:07:00')", "ts_dt_f", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') >= DATE('2020-09-16')", "dt_d_t", "boolean"),
        schema("DATE('2020-09-16') >= DATETIME('1961-04-12 09:07:00')", "d_dt_t", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') >= DATE('2020-09-16')", "dt_d_f", "boolean"),
        schema("DATE('1961-04-12') >= DATETIME('1984-12-15 22:15:07')", "d_dt_f", "boolean"),
        schema("DATETIME('" + today + " 10:20:30') >= TIME('10:20:30')", "dt_t_t", "boolean"),
        schema("TIME('20:40:50') >= DATETIME('" + today + " 10:20:30')", "t_dt_t", "boolean"),
        schema("DATETIME('1961-04-12 09:07:00') >= TIME('09:07:00')", "dt_t_f", "boolean"),
        schema("TIME('09:07:00') >= DATETIME('3077-12-15 22:15:07')", "t_dt_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGteCompareDateWithOtherTypes() throws IOException {
    JSONObject result = executeQuery("select "
        + "DATE('2020-09-16') >= TIMESTAMP('2020-09-16 00:00:00') AS `d_ts_t`, "
        + "TIMESTAMP('2077-04-12 09:07:00') >= DATE('2020-09-16') AS `ts_d_t`, "
        + "DATE('1961-04-12') >= TIMESTAMP('1961-04-12 09:07:00') AS `d_ts_f`, "
        + "TIMESTAMP('1961-04-12 09:07:00') >= DATE('1984-12-15') AS `ts_d_f`, "
        + "DATE('2020-09-16') >= DATETIME('2020-09-16 00:00:00') AS `d_dt_t`, "
        + "DATETIME('2020-09-16 00:00:00') >= DATE('1984-03-22') AS `dt_d_t`, "
        + "DATE('1960-12-15') >= DATETIME('1961-04-12 09:07:00') AS `d_dt_f`, "
        + "DATETIME('1961-04-12 10:20:30') >= DATE('1984-11-15') AS `dt_d_f`, "
        + "DATE('3077-04-12') >= TIME('00:00:00') AS `d_t_t`, "
        + "TIME('00:00:00') >= DATE('2020-09-16') AS `t_d_t`, "
        + "DATE('2020-09-16') >= TIME('09:07:00') AS `d_t_f`, "
        + "TIME('09:07:00') >= DATE('3077-04-12') AS `t_d_f` ");
    verifySchema(result,
        schema("DATE('2020-09-16') >= TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", "boolean"),
        schema("TIMESTAMP('2077-04-12 09:07:00') >= DATE('2020-09-16')", "ts_d_t", "boolean"),
        schema("DATE('1961-04-12') >= TIMESTAMP('1961-04-12 09:07:00')", "d_ts_f", "boolean"),
        schema("TIMESTAMP('1961-04-12 09:07:00') >= DATE('1984-12-15')", "ts_d_f", "boolean"),
        schema("DATE('2020-09-16') >= DATETIME('2020-09-16 00:00:00')", "d_dt_t", "boolean"),
        schema("DATETIME('2020-09-16 00:00:00') >= DATE('1984-03-22')", "dt_d_t", "boolean"),
        schema("DATE('1960-12-15') >= DATETIME('1961-04-12 09:07:00')", "d_dt_f", "boolean"),
        schema("DATETIME('1961-04-12 10:20:30') >= DATE('1984-11-15')", "dt_d_f", "boolean"),
        schema("DATE('3077-04-12') >= TIME('00:00:00')", "d_t_t", "boolean"),
        schema("TIME('00:00:00') >= DATE('2020-09-16')", "t_d_t", "boolean"),
        schema("DATE('2020-09-16') >= TIME('09:07:00')", "d_t_f", "boolean"),
        schema("TIME('09:07:00') >= DATE('3077-04-12')", "t_d_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGteCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery("select "
        + "TIME('10:20:30') >= DATETIME('" + today + " 10:20:30') AS `t_dt_t`, "
        + "DATETIME('" + today + " 20:40:50') >= TIME('10:20:30') AS `dt_t_t`, "
        + "TIME('09:07:00') >= DATETIME('3077-04-12 09:07:00') AS `t_dt_f`, "
        + "DATETIME('" + today + " 09:07:00') >= TIME('10:20:30') AS `dt_t_f`, "
        + "TIME('10:20:30') >= TIMESTAMP('" + today + " 10:20:30') AS `t_ts_t`, "
        + "TIMESTAMP('" + today + " 20:50:42') >= TIME('10:20:30') AS `ts_t_t`, "
        + "TIME('22:15:07') >= TIMESTAMP('3077-12-15 22:15:07') AS `t_ts_f`, "
        + "TIMESTAMP('1984-12-15 10:20:30') >= TIME('10:20:30') AS `ts_t_f`, "
        + "TIME('00:00:00') >= DATE('1961-04-12') AS `t_d_t`, "
        + "DATE('3077-04-12') >= TIME('10:20:30') AS `d_t_t`, "
        + "TIME('09:07:00') >= DATE('3077-04-12') AS `t_d_f`, "
        + "DATE('2020-09-16') >= TIME('09:07:00') AS `d_t_f` ");
    verifySchema(result,
        schema("TIME('10:20:30') >= DATETIME('" + today + " 10:20:30')", "t_dt_t", "boolean"),
        schema("DATETIME('" + today + " 20:40:50') >= TIME('10:20:30')", "dt_t_t", "boolean"),
        schema("TIME('09:07:00') >= DATETIME('3077-04-12 09:07:00')", "t_dt_f", "boolean"),
        schema("DATETIME('" + today + " 09:07:00') >= TIME('10:20:30')", "dt_t_f", "boolean"),
        schema("TIME('10:20:30') >= TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", "boolean"),
        schema("TIMESTAMP('" + today + " 20:50:42') >= TIME('10:20:30')", "ts_t_t", "boolean"),
        schema("TIME('22:15:07') >= TIMESTAMP('3077-12-15 22:15:07')", "t_ts_f", "boolean"),
        schema("TIMESTAMP('1984-12-15 10:20:30') >= TIME('10:20:30')", "ts_t_f", "boolean"),
        schema("TIME('00:00:00') >= DATE('1961-04-12')", "t_d_t", "boolean"),
        schema("DATE('3077-04-12') >= TIME('10:20:30')", "d_t_t", "boolean"),
        schema("TIME('09:07:00') >= DATE('3077-04-12')", "t_d_f", "boolean"),
        schema("DATE('2020-09-16') >= TIME('09:07:00')", "d_t_f", "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
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
