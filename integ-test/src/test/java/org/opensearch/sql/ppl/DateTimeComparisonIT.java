/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.time.LocalDate;
import java.util.TimeZone;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;

public class DateTimeComparisonIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.DATA_TYPE_NONNUMERIC);
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
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`eq1` = DATE('2020-09-16') = DATE('2020-09-16'), "
        + "`eq2` = DATE('2020-09-16') = DATE('1961-04-12'), "
        + "`neq1` = DATE('2020-09-16') != DATE('1984-12-15'), "
        + "`neq2` = DATE('1961-04-12') != DATE('1984-12-15'), "
        + "`neq3` = DATE('1961-04-12') != DATE('1961-04-12'), "
        + "`gt1` = DATE('1984-12-15') > DATE('1961-04-12'), "
        + "`gt2` = DATE('1984-12-15') > DATE('2020-09-16'), "
        + "`lt1` = DATE('1961-04-12') < DATE('1984-12-15'), "
        + "`lt2` = DATE('1984-12-15') < DATE('1961-04-12'), "
        + "`gte1` = DATE('1984-12-15') >= DATE('1961-04-12'), "
        + "`gte2` = DATE('1984-12-15') >= DATE('1984-12-15'), "
        + "`gte3` = DATE('1984-12-15') >= DATE('2020-09-16'), "
        + "`lte1` = DATE('1961-04-12') <= DATE('1984-12-15'), "
        + "`lte2` = DATE('1961-04-12') <= DATE('1961-04-12'), "
        + "`lte3` = DATE('2020-09-16') <= DATE('1961-04-12') "
        + " | fields `eq1`, `eq2`, `neq1`, `neq2`, `neq3`, `gt1`, `gt2`, `lt1`, `lt2`, `gte1`,"
                + " `gte2`, `gte3`, `lte1`, `lte2`, `lte3`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("eq1", null, "boolean"), schema("eq2", null, "boolean"),
        schema("neq1", null, "boolean"), schema("neq2", null, "boolean"),
        schema("neq3", null, "boolean"), schema("gt1", null, "boolean"),
        schema("gt2", null, "boolean"), schema("lt1", null, "boolean"),
        schema("lt2", null, "boolean"), schema("gte1", null, "boolean"),
        schema("gte2", null, "boolean"), schema("gte3", null, "boolean"),
        schema("lte1", null, "boolean"), schema("lte2", null, "boolean"),
        schema("lte3", null, "boolean"));
    verifyDataRows(result,
        rows(true, false, true, true, false, true, false,
            true, false, true, true, false, true, true, false));
  }

  @Test
  public void testCompareTimes() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`eq1` = TIME('09:16:37') = TIME('09:16:37'), "
        + "`eq2` = TIME('09:16:37') = TIME('04:12:42'), "
        + "`neq1` = TIME('09:16:37') != TIME('12:15:22'), "
        + "`neq2` = TIME('04:12:42') != TIME('12:15:22'), "
        + "`neq3` = TIME('04:12:42') != TIME('04:12:42'), "
        + "`gt1` = TIME('12:15:22') > TIME('04:12:42'), "
        + "`gt2` = TIME('12:15:22') > TIME('19:16:03'), "
        + "`lt1` = TIME('04:12:42') < TIME('12:15:22'), "
        + "`lt2` = TIME('14:12:38') < TIME('12:15:22'), "
        + "`gte1` = TIME('12:15:22') >= TIME('04:12:42'), "
        + "`gte2` = TIME('12:15:22') >= TIME('12:15:22'), "
        + "`gte3` = TIME('12:15:22') >= TIME('19:16:03'), "
        + "`lte1` = TIME('04:12:42') <= TIME('12:15:22'), "
        + "`lte2` = TIME('04:12:42') <= TIME('04:12:42'), "
        + "`lte3` = TIME('19:16:03') <= TIME('04:12:42') "
        + " | fields `eq1`, `eq2`, `neq1`, `neq2`, `neq3`, `gt1`, `gt2`, `lt1`, `lt2`, `gte1`,"
                + " `gte2`, `gte3`, `lte1`, `lte2`, `lte3`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("eq1", null, "boolean"), schema("eq2", null, "boolean"),
        schema("neq1", null, "boolean"), schema("neq2", null, "boolean"),
        schema("neq3", null, "boolean"), schema("gt1", null, "boolean"),
        schema("gt2", null, "boolean"), schema("lt1", null, "boolean"),
        schema("lt2", null, "boolean"), schema("gte1", null, "boolean"),
        schema("gte2", null, "boolean"), schema("gte3", null, "boolean"),
        schema("lte1", null, "boolean"), schema("lte2", null, "boolean"),
        schema("lte3", null, "boolean"));
    verifyDataRows(result,
        rows(true, false, true, true, false, true, false,
            true, false, true, true, false, true, true, false));
  }

  @Test
  public void testCompareDatetimes() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`eq1` = DATETIME('2020-09-16 10:20:30') = DATETIME('2020-09-16 10:20:30'), "
        + "`eq2` = DATETIME('2020-09-16 10:20:30') = DATETIME('1961-04-12 09:07:00'), "
        + "`neq1` = DATETIME('2020-09-16 10:20:30') != DATETIME('1984-12-15 22:15:07'), "
        + "`neq2` = DATETIME('1984-12-15 22:15:08') != DATETIME('1984-12-15 22:15:07'), "
        + "`neq3` = DATETIME('1961-04-12 09:07:00') != DATETIME('1961-04-12 09:07:00'), "
        + "`gt1` = DATETIME('1984-12-15 22:15:07') > DATETIME('1961-04-12 22:15:07'), "
        + "`gt2` = DATETIME('1984-12-15 22:15:07') > DATETIME('1984-12-15 22:15:06'), "
        + "`gt3` = DATETIME('1984-12-15 22:15:07') > DATETIME('2020-09-16 10:20:30'), "
        + "`lt1` = DATETIME('1961-04-12 09:07:00') < DATETIME('1984-12-15 09:07:00'), "
        + "`lt2` = DATETIME('1984-12-15 22:15:07') < DATETIME('1984-12-15 22:15:08'), "
        + "`lt3` = DATETIME('1984-12-15 22:15:07') < DATETIME('1961-04-12 09:07:00'), "
        + "`gte1` = DATETIME('1984-12-15 22:15:07') >= DATETIME('1961-04-12 09:07:00'), "
        + "`gte2` = DATETIME('1984-12-15 22:15:07') >= DATETIME('1984-12-15 22:15:07'), "
        + "`gte3` = DATETIME('1984-12-15 22:15:07') >= DATETIME('2020-09-16 10:20:30'), "
        + "`lte1` = DATETIME('1961-04-12 09:07:00') <= DATETIME('1984-12-15 22:15:07'), "
        + "`lte2` = DATETIME('1961-04-12 09:07:00') <= DATETIME('1961-04-12 09:07:00'), "
        + "`lte3` = DATETIME('2020-09-16 10:20:30') <= DATETIME('1961-04-12 09:07:00') "
        + " | fields `eq1`, `eq2`, `neq1`, `neq2`, `neq3`, `gt1`, `gt2`, `gt3`, `lt1`, `lt2`,"
                + "`lt3`, `gte1`, `gte2`, `gte3`, `lte1`, `lte2`, `lte3`",
            TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("eq1", null, "boolean"), schema("eq2", null, "boolean"),
        schema("neq1", null, "boolean"), schema("neq2", null, "boolean"),
        schema("neq3", null, "boolean"), schema("gt1", null, "boolean"),
        schema("gt2", null, "boolean"), schema("gt3", null, "boolean"),
        schema("lt1", null, "boolean"), schema("lt2", null, "boolean"),
        schema("lt3", null, "boolean"), schema("gte1", null, "boolean"),
        schema("gte2", null, "boolean"), schema("gte3", null, "boolean"),
        schema("lte1", null, "boolean"), schema("lte2", null, "boolean"),
        schema("lte3", null, "boolean"));
    verifyDataRows(result,
        rows(true, false, true, true, false, true, true, false,
            true, true, false, true, true, false, true, true, false));
  }

  @Test
  public void testCompareTimestamps() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`eq1` = TIMESTAMP('2020-09-16 10:20:30') = TIMESTAMP('2020-09-16 10:20:30'), "
        + "`eq2` = TIMESTAMP('2020-09-16 10:20:30') = TIMESTAMP('1961-04-12 09:07:00'), "
        + "`neq1` = TIMESTAMP('2020-09-16 10:20:30') != TIMESTAMP('1984-12-15 22:15:07'), "
        + "`neq2` = TIMESTAMP('1984-12-15 22:15:08') != TIMESTAMP('1984-12-15 22:15:07'), "
        + "`neq3` = TIMESTAMP('1961-04-12 09:07:00') != TIMESTAMP('1961-04-12 09:07:00'), "
        + "`gt1` = TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('1961-04-12 22:15:07'), "
        + "`gt2` = TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('1984-12-15 22:15:06'), "
        + "`gt3` = TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('2020-09-16 10:20:30'), "
        + "`lt1` = TIMESTAMP('1961-04-12 09:07:00') < TIMESTAMP('1984-12-15 09:07:00'), "
        + "`lt2` = TIMESTAMP('1984-12-15 22:15:07') < TIMESTAMP('1984-12-15 22:15:08'), "
        + "`lt3` = TIMESTAMP('1984-12-15 22:15:07') < TIMESTAMP('1961-04-12 09:07:00'), "
        + "`gte1` = TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('1961-04-12 09:07:00'), "
        + "`gte2` = TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('1984-12-15 22:15:07'), "
        + "`gte3` = TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('2020-09-16 10:20:30'), "
        + "`lte1` = TIMESTAMP('1961-04-12 09:07:00') <= TIMESTAMP('1984-12-15 22:15:07'), "
        + "`lte2` = TIMESTAMP('1961-04-12 09:07:00') <= TIMESTAMP('1961-04-12 09:07:00'), "
        + "`lte3` = TIMESTAMP('2020-09-16 10:20:30') <= TIMESTAMP('1961-04-12 09:07:00') "
        + " | fields `eq1`, `eq2`, `neq1`, `neq2`, `neq3`, `gt1`, `gt2`, `gt3`, `lt1`, `lt2`,"
                + "`lt3`, `gte1`, `gte2`, `gte3`, `lte1`, `lte2`, `lte3`",
            TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("eq1", null, "boolean"), schema("eq2", null, "boolean"),
        schema("neq1", null, "boolean"), schema("neq2", null, "boolean"),
        schema("neq3", null, "boolean"), schema("gt1", null, "boolean"),
        schema("gt2", null, "boolean"), schema("gt3", null, "boolean"),
        schema("lt1", null, "boolean"), schema("lt2", null, "boolean"),
        schema("lt3", null, "boolean"), schema("gte1", null, "boolean"),
        schema("gte2", null, "boolean"), schema("gte3", null, "boolean"),
        schema("lte1", null, "boolean"), schema("lte2", null, "boolean"),
        schema("lte3", null, "boolean"));
    verifyDataRows(result,
        rows(true, false, true, true, false, true, true, false,
            true, true, false, true, true, false, true, true, false));
  }

  @Test
  public void testEqCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`ts_dt_t` = TIMESTAMP('2020-09-16 10:20:30') = DATETIME('2020-09-16 10:20:30'), "
        + "`dt_ts_t` = DATETIME('2020-09-16 10:20:30') = TIMESTAMP('2020-09-16 10:20:30'), "
        + "`ts_dt_f` = TIMESTAMP('2020-09-16 10:20:30') = DATETIME('1961-04-12 09:07:00'), "
        + "`dt_ts_f` = DATETIME('1961-04-12 09:07:00') = TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_d_t` = TIMESTAMP('2020-09-16 00:00:00') = DATE('2020-09-16'), "
        + "`d_ts_t` = DATE('2020-09-16') = TIMESTAMP('2020-09-16 00:00:00'), "
        + "`ts_d_f` = TIMESTAMP('2020-09-16 10:20:30') = DATE('1961-04-12'), "
        + "`d_ts_f` = DATE('1961-04-12') = TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_t_t` = TIMESTAMP('" + today + " 10:20:30') = TIME('10:20:30'), "
        + "`t_ts_t` = TIME('10:20:30') = TIMESTAMP('" + today + " 10:20:30'), "
        + "`ts_t_f` = TIMESTAMP('2020-09-16 10:20:30') = TIME('09:07:00'), "
        + "`t_ts_f` = TIME('09:07:00') = TIMESTAMP('1984-12-15 22:15:07') "
        + " | fields `ts_dt_t`, `dt_ts_t`, `ts_dt_f`, `dt_ts_f`, `ts_d_t`, `d_ts_t`, `ts_d_f`,"
            + "`d_ts_f`, `ts_t_t`, `t_ts_t`, `ts_t_f`, `t_ts_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("ts_dt_t", null, "boolean"), schema("dt_ts_t", null, "boolean"),
        schema("ts_dt_f", null, "boolean"), schema("dt_ts_f", null, "boolean"),
        schema("ts_d_t", null, "boolean"), schema("d_ts_t", null, "boolean"),
        schema("ts_d_f", null, "boolean"), schema("d_ts_f", null, "boolean"),
        schema("ts_t_t", null, "boolean"), schema("t_ts_t", null, "boolean"),
        schema("ts_t_f", null, "boolean"), schema("t_ts_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testEqCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`dt_ts_t` = DATETIME('2020-09-16 10:20:30') = TIMESTAMP('2020-09-16 10:20:30'), "
        + "`ts_dt_t` = TIMESTAMP('2020-09-16 10:20:30') = DATETIME('2020-09-16 10:20:30'), "
        + "`dt_ts_f` = DATETIME('2020-09-16 10:20:30') = TIMESTAMP('1961-04-12 09:07:00'), "
        + "`ts_dt_f` = TIMESTAMP('1961-04-12 09:07:00') = DATETIME('1984-12-15 22:15:07'), "
        + "`dt_d_t` = DATETIME('2020-09-16 00:00:00') = DATE('2020-09-16'), "
        + "`d_dt_t` = DATE('2020-09-16') = DATETIME('2020-09-16 00:00:00'), "
        + "`dt_d_f` = DATETIME('2020-09-16 10:20:30') = DATE('1961-04-12'), "
        + "`d_dt_f` = DATE('1961-04-12') = DATETIME('1984-12-15 22:15:07'), "
        + "`dt_t_t` = DATETIME('" + today + " 10:20:30') = TIME('10:20:30'), "
        + "`t_dt_t` = TIME('10:20:30') = DATETIME('" + today + " 10:20:30'), "
        + "`dt_t_f` = DATETIME('2020-09-16 10:20:30') = TIME('09:07:00'), "
        + "`t_dt_f` = TIME('09:07:00') = DATETIME('1984-12-15 22:15:07') "
        + " | fields `dt_ts_t`, `ts_dt_t`, `dt_ts_f`, `ts_dt_f`, `dt_d_t`, `d_dt_t`, `dt_d_f`,"
            + "`d_dt_f`, `dt_t_t`, `t_dt_t`, `dt_t_f`, `t_dt_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("dt_ts_t", null, "boolean"), schema("ts_dt_t", null, "boolean"),
        schema("dt_ts_f", null, "boolean"), schema("ts_dt_f", null, "boolean"),
        schema("dt_d_t", null, "boolean"), schema("d_dt_t", null, "boolean"),
        schema("dt_d_f", null, "boolean"), schema("d_dt_f", null, "boolean"),
        schema("dt_t_t", null, "boolean"), schema("t_dt_t", null, "boolean"),
        schema("dt_t_f", null, "boolean"), schema("t_dt_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testEqCompareDateWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`d_ts_t` = DATE('2020-09-16') = TIMESTAMP('2020-09-16 00:00:00'), "
        + "`ts_d_t` = TIMESTAMP('2020-09-16 00:00:00') = DATE('2020-09-16'), "
        + "`d_ts_f` = DATE('2020-09-16') = TIMESTAMP('1961-04-12 09:07:00'), "
        + "`ts_d_f` = TIMESTAMP('1984-12-15 09:07:00') = DATE('1984-12-15'), "
        + "`d_dt_t` = DATE('2020-09-16') = DATETIME('2020-09-16 00:00:00'), "
        + "`dt_d_t` = DATETIME('2020-09-16 00:00:00') = DATE('2020-09-16'), "
        + "`d_dt_f` = DATE('1961-04-12') = DATETIME('1984-12-15 22:15:07'), "
        + "`dt_d_f` = DATETIME('1961-04-12 10:20:30') = DATE('1961-04-12'), "
        + "`d_t_t` = DATE('" + today + "') = TIME('00:00:00'), "
        + "`t_d_t` = TIME('00:00:00') = DATE('" + today + "'), "
        + "`d_t_f` = DATE('2020-09-16') = TIME('09:07:00'), "
        + "`t_d_f` = TIME('09:07:00') = DATE('" + today + "') "
        + " | fields `d_ts_t`, `ts_d_t`, `d_ts_f`, `ts_d_f`, `d_dt_t`, `dt_d_t`, `d_dt_f`,"
            + " `dt_d_f`, `d_t_t`, `t_d_t`, `d_t_f`, `t_d_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("d_ts_t", null, "boolean"), schema("ts_d_t", null, "boolean"),
        schema("d_ts_f", null, "boolean"), schema("ts_d_f", null, "boolean"),
        schema("d_dt_t", null, "boolean"), schema("dt_d_t", null, "boolean"),
        schema("d_dt_f", null, "boolean"), schema("dt_d_f", null, "boolean"),
        schema("d_t_t", null, "boolean"), schema("t_d_t", null, "boolean"),
        schema("d_t_f", null, "boolean"), schema("t_d_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testEqCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`t_dt_t` = TIME('10:20:30') = DATETIME('" + today + " 10:20:30'), "
        + "`dt_t_t` = DATETIME('" + today + " 10:20:30') = TIME('10:20:30'), "
        + "`t_dt_f` = TIME('09:07:00') = DATETIME('1961-04-12 09:07:00'), "
        + "`dt_t_f` = DATETIME('" + today + " 09:07:00') = TIME('10:20:30'), "
        + "`t_ts_t` = TIME('10:20:30') = TIMESTAMP('" + today + " 10:20:30'), "
        + "`ts_t_t` = TIMESTAMP('" + today + " 10:20:30') = TIME('10:20:30'), "
        + "`t_ts_f` = TIME('22:15:07') = TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_t_f` = TIMESTAMP('1984-12-15 10:20:30') = TIME('10:20:30'), "
        + "`t_d_t` = TIME('00:00:00') = DATE('" + today + "'), "
        + "`d_t_t` = DATE('" + today + "') = TIME('00:00:00'), "
        + "`t_d_f` = TIME('09:07:00') = DATE('" + today + "'), "
        + "`d_t_f` = DATE('2020-09-16') = TIME('09:07:00') "
        + " | fields `t_dt_t`, `dt_t_t`, `t_dt_f`, `dt_t_f`, `t_ts_t`, `ts_t_t`, `t_ts_f`,"
            + " `ts_t_f`, `t_d_t`, `d_t_t`, `t_d_f`, `d_t_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("t_dt_t", null, "boolean"), schema("dt_t_t", null, "boolean"),
        schema("t_dt_f", null, "boolean"), schema("dt_t_f", null, "boolean"),
        schema("t_ts_t", null, "boolean"), schema("ts_t_t", null, "boolean"),
        schema("t_ts_f", null, "boolean"), schema("ts_t_f", null, "boolean"),
        schema("t_d_t", null, "boolean"), schema("d_t_t", null, "boolean"),
        schema("t_d_f", null, "boolean"), schema("d_t_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testNeqCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`ts_dt_t` = TIMESTAMP('2020-09-16 10:20:30') != DATETIME('1961-04-12 09:07:00'), "
        + "`dt_ts_t` = DATETIME('1961-04-12 09:07:00') != TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_dt_f` = TIMESTAMP('2020-09-16 10:20:30') != DATETIME('2020-09-16 10:20:30'), "
        + "`dt_ts_f` = DATETIME('2020-09-16 10:20:30') != TIMESTAMP('2020-09-16 10:20:30'), "
        + "`ts_d_t` = TIMESTAMP('2020-09-16 10:20:30') != DATE('1961-04-12'), "
        + "`d_ts_t` = DATE('1961-04-12') != TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_d_f` = TIMESTAMP('2020-09-16 00:00:00') != DATE('2020-09-16'), "
        + "`d_ts_f` = DATE('2020-09-16') != TIMESTAMP('2020-09-16 00:00:00'), "
        + "`ts_t_t` = TIMESTAMP('2020-09-16 10:20:30') != TIME('09:07:00'), "
        + "`t_ts_t` = TIME('09:07:00') != TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_t_f` = TIMESTAMP('" + today + " 10:20:30') != TIME('10:20:30'), "
        + "`t_ts_f` = TIME('10:20:30') != TIMESTAMP('" + today + " 10:20:30') "
        + " | fields `ts_dt_t`, `dt_ts_t`, `ts_dt_f`, `dt_ts_f`, `ts_d_t`, `d_ts_t`, `ts_d_f`,"
            + "`d_ts_f`, `ts_t_t`, `t_ts_t`, `ts_t_f`, `t_ts_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("ts_dt_t", null, "boolean"), schema("dt_ts_t", null, "boolean"),
        schema("ts_dt_f", null, "boolean"), schema("dt_ts_f", null, "boolean"),
        schema("ts_d_t", null, "boolean"), schema("d_ts_t", null, "boolean"),
        schema("ts_d_f", null, "boolean"), schema("d_ts_f", null, "boolean"),
        schema("ts_t_t", null, "boolean"), schema("t_ts_t", null, "boolean"),
        schema("ts_t_f", null, "boolean"), schema("t_ts_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testNeqCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`dt_ts_t` = DATETIME('2020-09-16 10:20:30') != TIMESTAMP('1961-04-12 09:07:00'), "
        + "`ts_dt_t` = TIMESTAMP('1961-04-12 09:07:00') != DATETIME('1984-12-15 22:15:07'), "
        + "`dt_ts_f` = DATETIME('2020-09-16 10:20:30') != TIMESTAMP('2020-09-16 10:20:30'), "
        + "`ts_dt_f` = TIMESTAMP('2020-09-16 10:20:30') != DATETIME('2020-09-16 10:20:30'), "
        + "`dt_d_t` = DATETIME('2020-09-16 10:20:30') != DATE('1961-04-12'), "
        + "`d_dt_t` = DATE('1961-04-12') != DATETIME('1984-12-15 22:15:07'), "
        + "`dt_d_f` = DATETIME('2020-09-16 00:00:00') != DATE('2020-09-16'), "
        + "`d_dt_f` = DATE('2020-09-16') != DATETIME('2020-09-16 00:00:00'), "
        + "`dt_t_t` = DATETIME('2020-09-16 10:20:30') != TIME('09:07:00'), "
        + "`t_dt_t` = TIME('09:07:00') != DATETIME('1984-12-15 22:15:07'), "
        + "`dt_t_f` = DATETIME('" + today + " 10:20:30') != TIME('10:20:30'), "
        + "`t_dt_f` = TIME('10:20:30') != DATETIME('" + today + " 10:20:30') "
        + " | fields `dt_ts_t`, `ts_dt_t`, `dt_ts_f`, `ts_dt_f`, `dt_d_t`, `d_dt_t`, `dt_d_f`,"
            + "`d_dt_f`, `dt_t_t`, `t_dt_t`, `dt_t_f`, `t_dt_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("dt_ts_t", null, "boolean"), schema("ts_dt_t", null, "boolean"),
        schema("dt_ts_f", null, "boolean"), schema("ts_dt_f", null, "boolean"),
        schema("dt_d_t", null, "boolean"), schema("d_dt_t", null, "boolean"),
        schema("dt_d_f", null, "boolean"), schema("d_dt_f", null, "boolean"),
        schema("dt_t_t", null, "boolean"), schema("t_dt_t", null, "boolean"),
        schema("dt_t_f", null, "boolean"), schema("t_dt_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testNeqCompareDateWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`d_ts_t` = DATE('2020-09-16') != TIMESTAMP('1961-04-12 09:07:00'), "
        + "`ts_d_t` = TIMESTAMP('1984-12-15 09:07:00') != DATE('1984-12-15'), "
        + "`d_ts_f` = DATE('2020-09-16') != TIMESTAMP('2020-09-16 00:00:00'), "
        + "`ts_d_f` = TIMESTAMP('2020-09-16 00:00:00') != DATE('2020-09-16'), "
        + "`d_dt_t` = DATE('1961-04-12') != DATETIME('1984-12-15 22:15:07'), "
        + "`dt_d_t` = DATETIME('1961-04-12 10:20:30') != DATE('1961-04-12'), "
        + "`d_dt_f` = DATE('2020-09-16') != DATETIME('2020-09-16 00:00:00'), "
        + "`dt_d_f` = DATETIME('2020-09-16 00:00:00') != DATE('2020-09-16'), "
        + "`d_t_t` = DATE('2020-09-16') != TIME('09:07:00'), "
        + "`t_d_t` = TIME('09:07:00') != DATE('" + today + "'), "
        + "`d_t_f` = DATE('" + today + "') != TIME('00:00:00'), "
        + "`t_d_f` = TIME('00:00:00') != DATE('" + today + "') "
        + " | fields `d_ts_t`, `ts_d_t`, `d_ts_f`, `ts_d_f`, `d_dt_t`, `dt_d_t`, `d_dt_f`,"
            + " `dt_d_f`, `d_t_t`, `t_d_t`, `d_t_f`, `t_d_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("d_ts_t", null, "boolean"), schema("ts_d_t", null, "boolean"),
        schema("d_ts_f", null, "boolean"), schema("ts_d_f", null, "boolean"),
        schema("d_dt_t", null, "boolean"), schema("dt_d_t", null, "boolean"),
        schema("d_dt_f", null, "boolean"), schema("dt_d_f", null, "boolean"),
        schema("d_t_t", null, "boolean"), schema("t_d_t", null, "boolean"),
        schema("d_t_f", null, "boolean"), schema("t_d_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testNeqCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`t_dt_t` = TIME('09:07:00') != DATETIME('1961-04-12 09:07:00'), "
        + "`dt_t_t` = DATETIME('" + today + " 09:07:00') != TIME('10:20:30'), "
        + "`t_dt_f` = TIME('10:20:30') != DATETIME('" + today + " 10:20:30'), "
        + "`dt_t_f` = DATETIME('" + today + " 10:20:30') != TIME('10:20:30'), "
        + "`t_ts_t` = TIME('22:15:07') != TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_t_t` = TIMESTAMP('1984-12-15 10:20:30') != TIME('10:20:30'), "
        + "`t_ts_f` = TIME('10:20:30') != TIMESTAMP('" + today + " 10:20:30'), "
        + "`ts_t_f` = TIMESTAMP('" + today + " 10:20:30') != TIME('10:20:30'), "
        + "`t_d_t` = TIME('09:07:00') != DATE('" + today + "'), "
        + "`d_t_t` = DATE('2020-09-16') != TIME('09:07:00'), "
        + "`t_d_f` = TIME('00:00:00') != DATE('" + today + "'), "
        + "`d_t_f` = DATE('" + today + "') != TIME('00:00:00') "
        + " | fields `t_dt_t`, `dt_t_t`, `t_dt_f`, `dt_t_f`, `t_ts_t`, `ts_t_t`, `t_ts_f`,"
            + " `ts_t_f`, `t_d_t`, `d_t_t`, `t_d_f`, `d_t_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("t_dt_t", null, "boolean"), schema("dt_t_t", null, "boolean"),
        schema("t_dt_f", null, "boolean"), schema("dt_t_f", null, "boolean"),
        schema("t_ts_t", null, "boolean"), schema("ts_t_t", null, "boolean"),
        schema("t_ts_f", null, "boolean"), schema("ts_t_f", null, "boolean"),
        schema("t_d_t", null, "boolean"), schema("d_t_t", null, "boolean"),
        schema("t_d_f", null, "boolean"), schema("d_t_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLtCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`ts_dt_t` = TIMESTAMP('2020-09-16 10:20:30') < DATETIME('2061-04-12 09:07:00'), "
        + "`dt_ts_t` = DATETIME('1961-04-12 09:07:00') < TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_dt_f` = TIMESTAMP('2020-09-16 10:20:30') < DATETIME('2020-09-16 10:20:30'), "
        + "`dt_ts_f` = DATETIME('2020-09-16 10:20:30') < TIMESTAMP('1961-04-12 09:07:00'), "
        + "`ts_d_t` = TIMESTAMP('2020-09-16 10:20:30') < DATE('2077-04-12'), "
        + "`d_ts_t` = DATE('1961-04-12') < TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_d_f` = TIMESTAMP('2020-09-16 10:20:30') < DATE('1961-04-12'), "
        + "`d_ts_f` = DATE('2020-09-16') < TIMESTAMP('2020-09-16 00:00:00'), "
        + "`ts_t_t` = TIMESTAMP('2020-09-16 10:20:30') < TIME('09:07:00'), "
        + "`t_ts_t` = TIME('09:07:00') < TIMESTAMP('3077-12-15 22:15:07'), "
        + "`ts_t_f` = TIMESTAMP('" + today + " 10:20:30') < TIME('10:20:30'), "
        + "`t_ts_f` = TIME('20:50:40') < TIMESTAMP('" + today + " 10:20:30') "
        + " | fields `ts_dt_t`, `dt_ts_t`, `ts_dt_f`, `dt_ts_f`, `ts_d_t`, `d_ts_t`, `ts_d_f`,"
            + "`d_ts_f`, `ts_t_t`, `t_ts_t`, `ts_t_f`, `t_ts_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("ts_dt_t", null, "boolean"), schema("dt_ts_t", null, "boolean"),
        schema("ts_dt_f", null, "boolean"), schema("dt_ts_f", null, "boolean"),
        schema("ts_d_t", null, "boolean"), schema("d_ts_t", null, "boolean"),
        schema("ts_d_f", null, "boolean"), schema("d_ts_f", null, "boolean"),
        schema("ts_t_t", null, "boolean"), schema("t_ts_t", null, "boolean"),
        schema("ts_t_f", null, "boolean"), schema("t_ts_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLtCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`dt_ts_t` = DATETIME('2020-09-16 10:20:30') < TIMESTAMP('2077-04-12 09:07:00'), "
        + "`ts_dt_t` = TIMESTAMP('1961-04-12 09:07:00') < DATETIME('1984-12-15 22:15:07'), "
        + "`dt_ts_f` = DATETIME('2020-09-16 10:20:30') < TIMESTAMP('2020-09-16 10:20:30'), "
        + "`ts_dt_f` = TIMESTAMP('2020-09-16 10:20:30') < DATETIME('1984-12-15 22:15:07'), "
        + "`dt_d_t` = DATETIME('2020-09-16 10:20:30') < DATE('3077-04-12'), "
        + "`d_dt_t` = DATE('1961-04-12') < DATETIME('1984-12-15 22:15:07'), "
        + "`dt_d_f` = DATETIME('2020-09-16 00:00:00') < DATE('2020-09-16'), "
        + "`d_dt_f` = DATE('2020-09-16') < DATETIME('1961-04-12 09:07:00'), "
        + "`dt_t_t` = DATETIME('2020-09-16 10:20:30') < TIME('09:07:00'), "
        + "`t_dt_t` = TIME('09:07:00') < DATETIME('3077-12-15 22:15:07'), "
        + "`dt_t_f` = DATETIME('" + today + " 10:20:30') < TIME('10:20:30'), "
        + "`t_dt_f` = TIME('20:40:50') < DATETIME('" + today + " 10:20:30') "
        + " | fields `dt_ts_t`, `ts_dt_t`, `dt_ts_f`, `ts_dt_f`, `dt_d_t`, `d_dt_t`, `dt_d_f`,"
            + "`d_dt_f`, `dt_t_t`, `t_dt_t`, `dt_t_f`, `t_dt_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("dt_ts_t", null, "boolean"), schema("ts_dt_t", null, "boolean"),
        schema("dt_ts_f", null, "boolean"), schema("ts_dt_f", null, "boolean"),
        schema("dt_d_t", null, "boolean"), schema("d_dt_t", null, "boolean"),
        schema("dt_d_f", null, "boolean"), schema("d_dt_f", null, "boolean"),
        schema("dt_t_t", null, "boolean"), schema("t_dt_t", null, "boolean"),
        schema("dt_t_f", null, "boolean"), schema("t_dt_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLtCompareDateWithOtherTypes() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`d_ts_t` = DATE('2020-09-16') < TIMESTAMP('3077-04-12 09:07:00'), "
        + "`ts_d_t` = TIMESTAMP('1961-04-12 09:07:00') < DATE('1984-12-15'), "
        + "`d_ts_f` = DATE('2020-09-16') < TIMESTAMP('2020-09-16 00:00:00'), "
        + "`ts_d_f` = TIMESTAMP('2077-04-12 09:07:00') < DATE('2020-09-16'), "
        + "`d_dt_t` = DATE('1961-04-12') < DATETIME('1984-12-15 22:15:07'), "
        + "`dt_d_t` = DATETIME('1961-04-12 10:20:30') < DATE('1984-11-15'), "
        + "`d_dt_f` = DATE('2020-09-16') < DATETIME('2020-09-16 00:00:00'), "
        + "`dt_d_f` = DATETIME('2020-09-16 00:00:00') < DATE('1984-03-22'), "
        + "`d_t_t` = DATE('2020-09-16') < TIME('09:07:00'), "
        + "`t_d_t` = TIME('09:07:00') < DATE('3077-04-12'), "
        + "`d_t_f` = DATE('3077-04-12') < TIME('00:00:00'), "
        + "`t_d_f` = TIME('00:00:00') < DATE('2020-09-16') "
        + " | fields `d_ts_t`, `ts_d_t`, `d_ts_f`, `ts_d_f`, `d_dt_t`, `dt_d_t`, `d_dt_f`,"
            + " `dt_d_f`, `d_t_t`, `t_d_t`, `d_t_f`, `t_d_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("d_ts_t", null, "boolean"), schema("ts_d_t", null, "boolean"),
        schema("d_ts_f", null, "boolean"), schema("ts_d_f", null, "boolean"),
        schema("d_dt_t", null, "boolean"), schema("dt_d_t", null, "boolean"),
        schema("d_dt_f", null, "boolean"), schema("dt_d_f", null, "boolean"),
        schema("d_t_t", null, "boolean"), schema("t_d_t", null, "boolean"),
        schema("d_t_f", null, "boolean"), schema("t_d_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLtCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`t_dt_t` = TIME('09:07:00') < DATETIME('3077-04-12 09:07:00'), "
        + "`dt_t_t` = DATETIME('" + today + " 09:07:00') < TIME('10:20:30'), "
        + "`t_dt_f` = TIME('10:20:30') < DATETIME('" + today + " 10:20:30'), "
        + "`dt_t_f` = DATETIME('" + today + " 20:40:50') < TIME('10:20:30'), "
        + "`t_ts_t` = TIME('22:15:07') < TIMESTAMP('3077-12-15 22:15:07'), "
        + "`ts_t_t` = TIMESTAMP('1984-12-15 10:20:30') < TIME('10:20:30'), "
        + "`t_ts_f` = TIME('10:20:30') < TIMESTAMP('" + today + " 10:20:30'), "
        + "`ts_t_f` = TIMESTAMP('" + today + " 20:50:42') < TIME('10:20:30'), "
        + "`t_d_t` = TIME('09:07:00') < DATE('3077-04-12'), "
        + "`d_t_t` = DATE('2020-09-16') < TIME('09:07:00'), "
        + "`t_d_f` = TIME('00:00:00') < DATE('1961-04-12'), "
        + "`d_t_f` = DATE('3077-04-12') < TIME('10:20:30') "
        + " | fields `t_dt_t`, `dt_t_t`, `t_dt_f`, `dt_t_f`, `t_ts_t`, `ts_t_t`, `t_ts_f`,"
            + " `ts_t_f`, `t_d_t`, `d_t_t`, `t_d_f`, `d_t_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("t_dt_t", null, "boolean"), schema("dt_t_t", null, "boolean"),
        schema("t_dt_f", null, "boolean"), schema("dt_t_f", null, "boolean"),
        schema("t_ts_t", null, "boolean"), schema("ts_t_t", null, "boolean"),
        schema("t_ts_f", null, "boolean"), schema("ts_t_f", null, "boolean"),
        schema("t_d_t", null, "boolean"), schema("d_t_t", null, "boolean"),
        schema("t_d_f", null, "boolean"), schema("d_t_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGtCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`ts_dt_t` = TIMESTAMP('2020-09-16 10:20:30') > DATETIME('2020-09-16 10:20:25'), "
        + "`dt_ts_t` = DATETIME('2020-09-16 10:20:30') > TIMESTAMP('1961-04-12 09:07:00'), "
        + "`ts_dt_f` = TIMESTAMP('2020-09-16 10:20:30') > DATETIME('2061-04-12 09:07:00'), "
        + "`dt_ts_f` = DATETIME('1961-04-12 09:07:00') > TIMESTAMP('1984-12-15 09:07:00'), "
        + "`ts_d_t` = TIMESTAMP('2020-09-16 10:20:30') > DATE('1961-04-12'), "
        + "`d_ts_t` = DATE('2020-09-16') > TIMESTAMP('2020-09-15 22:15:07'), "
        + "`ts_d_f` = TIMESTAMP('2020-09-16 10:20:30') > DATE('2077-04-12'), "
        + "`d_ts_f` = DATE('1961-04-12') > TIMESTAMP('1961-04-12 00:00:00'), "
        + "`ts_t_t` = TIMESTAMP('3077-07-08 20:20:30') > TIME('10:20:30'), "
        + "`t_ts_t` = TIME('20:50:40') > TIMESTAMP('" + today + " 10:20:30'), "
        + "`ts_t_f` = TIMESTAMP('" + today + " 10:20:30') > TIME('10:20:30'), "
        + "`t_ts_f` = TIME('09:07:00') > TIMESTAMP('3077-12-15 22:15:07') "
        + " | fields `ts_dt_t`, `dt_ts_t`, `ts_dt_f`, `dt_ts_f`, `ts_d_t`, `d_ts_t`, `ts_d_f`,"
            + "`d_ts_f`, `ts_t_t`, `t_ts_t`, `ts_t_f`, `t_ts_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("ts_dt_t", null, "boolean"), schema("dt_ts_t", null, "boolean"),
        schema("ts_dt_f", null, "boolean"), schema("dt_ts_f", null, "boolean"),
        schema("ts_d_t", null, "boolean"), schema("d_ts_t", null, "boolean"),
        schema("ts_d_f", null, "boolean"), schema("d_ts_f", null, "boolean"),
        schema("ts_t_t", null, "boolean"), schema("t_ts_t", null, "boolean"),
        schema("ts_t_f", null, "boolean"), schema("t_ts_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGtCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`dt_ts_t` = DATETIME('2020-09-16 10:20:31') > TIMESTAMP('2020-09-16 10:20:30'), "
        + "`ts_dt_t` = TIMESTAMP('2020-09-16 10:20:30') > DATETIME('1984-12-15 22:15:07'), "
        + "`dt_ts_f` = DATETIME('2020-09-16 10:20:30') > TIMESTAMP('2077-04-12 09:07:00'), "
        + "`ts_dt_f` = TIMESTAMP('1961-04-12 09:07:00') > DATETIME('1961-04-12 09:07:00'), "
        + "`dt_d_t` = DATETIME('3077-04-12 10:20:30') > DATE('2020-09-16'), "
        + "`d_dt_t` = DATE('2020-09-16') > DATETIME('1961-04-12 09:07:00'), "
        + "`dt_d_f` = DATETIME('2020-09-16 00:00:00') > DATE('2020-09-16'), "
        + "`d_dt_f` = DATE('1961-04-12') > DATETIME('1984-12-15 22:15:07'), "
        + "`dt_t_t` = DATETIME('3077-04-12 10:20:30') > TIME('09:07:00'), "
        + "`t_dt_t` = TIME('20:40:50') > DATETIME('" + today + " 10:20:30'), "
        + "`dt_t_f` = DATETIME('" + today + " 10:20:30') > TIME('10:20:30'), "
        + "`t_dt_f` = TIME('09:07:00') > DATETIME('3077-12-15 22:15:07') "
        + " | fields `dt_ts_t`, `ts_dt_t`, `dt_ts_f`, `ts_dt_f`, `dt_d_t`, `d_dt_t`, `dt_d_f`,"
            + "`d_dt_f`, `dt_t_t`, `t_dt_t`, `dt_t_f`, `t_dt_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("dt_ts_t", null, "boolean"), schema("ts_dt_t", null, "boolean"),
        schema("dt_ts_f", null, "boolean"), schema("ts_dt_f", null, "boolean"),
        schema("dt_d_t", null, "boolean"), schema("d_dt_t", null, "boolean"),
        schema("dt_d_f", null, "boolean"), schema("d_dt_f", null, "boolean"),
        schema("dt_t_t", null, "boolean"), schema("t_dt_t", null, "boolean"),
        schema("dt_t_f", null, "boolean"), schema("t_dt_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGtCompareDateWithOtherTypes() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`d_ts_t` = DATE('2020-09-16') > TIMESTAMP('1961-04-12 09:07:00'), "
        + "`ts_d_t` = TIMESTAMP('2077-04-12 09:07:00') > DATE('2020-09-16'), "
        + "`d_ts_f` = DATE('2020-09-16') > TIMESTAMP('2020-09-16 00:00:00'), "
        + "`ts_d_f` = TIMESTAMP('1961-04-12 09:07:00') > DATE('1984-12-15'), "
        + "`d_dt_t` = DATE('1984-12-15') > DATETIME('1961-04-12 09:07:00'), "
        + "`dt_d_t` = DATETIME('2020-09-16 00:00:00') > DATE('1984-03-22'), "
        + "`d_dt_f` = DATE('2020-09-16') > DATETIME('2020-09-16 00:00:00'), "
        + "`dt_d_f` = DATETIME('1961-04-12 10:20:30') > DATE('1984-11-15'), "
        + "`d_t_t` = DATE('3077-04-12') > TIME('00:00:00'), "
        + "`t_d_t` = TIME('00:00:00') > DATE('2020-09-16'), "
        + "`d_t_f` = DATE('2020-09-16') > TIME('09:07:00'), "
        + "`t_d_f` = TIME('09:07:00') > DATE('3077-04-12') "
        + " | fields `d_ts_t`, `ts_d_t`, `d_ts_f`, `ts_d_f`, `d_dt_t`, `dt_d_t`, `d_dt_f`,"
            + " `dt_d_f`, `d_t_t`, `t_d_t`, `d_t_f`, `t_d_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("d_ts_t", null, "boolean"), schema("ts_d_t", null, "boolean"),
        schema("d_ts_f", null, "boolean"), schema("ts_d_f", null, "boolean"),
        schema("d_dt_t", null, "boolean"), schema("dt_d_t", null, "boolean"),
        schema("d_dt_f", null, "boolean"), schema("dt_d_f", null, "boolean"),
        schema("d_t_t", null, "boolean"), schema("t_d_t", null, "boolean"),
        schema("d_t_f", null, "boolean"), schema("t_d_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGtCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`t_dt_t` = TIME('09:07:00') > DATETIME('1961-04-12 09:07:00'), "
        + "`dt_t_t` = DATETIME('" + today + " 20:40:50') > TIME('10:20:30'), "
        + "`t_dt_f` = TIME('10:20:30') > DATETIME('" + today + " 10:20:30'), "
        + "`dt_t_f` = DATETIME('" + today + " 09:07:00') > TIME('10:20:30'), "
        + "`t_ts_t` = TIME('22:15:07') > TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_t_t` = TIMESTAMP('" + today + " 20:50:42') > TIME('10:20:30'), "
        + "`t_ts_f` = TIME('10:20:30') > TIMESTAMP('" + today + " 10:20:30'), "
        + "`ts_t_f` = TIMESTAMP('1984-12-15 10:20:30') > TIME('10:20:30'), "
        + "`t_d_t` = TIME('00:00:00') > DATE('1961-04-12'), "
        + "`d_t_t` = DATE('3077-04-12') > TIME('10:20:30'), "
        + "`t_d_f` = TIME('09:07:00') > DATE('3077-04-12'), "
        + "`d_t_f` = DATE('2020-09-16') > TIME('09:07:00') "
        + " | fields `t_dt_t`, `dt_t_t`, `t_dt_f`, `dt_t_f`, `t_ts_t`, `ts_t_t`, `t_ts_f`,"
            + " `ts_t_f`, `t_d_t`, `d_t_t`, `t_d_f`, `d_t_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("t_dt_t", null, "boolean"), schema("dt_t_t", null, "boolean"),
        schema("t_dt_f", null, "boolean"), schema("dt_t_f", null, "boolean"),
        schema("t_ts_t", null, "boolean"), schema("ts_t_t", null, "boolean"),
        schema("t_ts_f", null, "boolean"), schema("ts_t_f", null, "boolean"),
        schema("t_d_t", null, "boolean"), schema("d_t_t", null, "boolean"),
        schema("t_d_f", null, "boolean"), schema("d_t_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLteCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`ts_dt_t` = TIMESTAMP('2020-09-16 10:20:30') <= DATETIME('2020-09-16 10:20:30'), "
        + "`dt_ts_t` = DATETIME('1961-04-12 09:07:00') <= TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_dt_f` = TIMESTAMP('2020-09-16 10:20:30') <= DATETIME('1961-04-12 09:07:00'), "
        + "`dt_ts_f` = DATETIME('2020-09-16 10:20:30') <= TIMESTAMP('1961-04-12 09:07:00'), "
        + "`ts_d_t` = TIMESTAMP('2020-09-16 10:20:30') <= DATE('2077-04-12'), "
        + "`d_ts_t` = DATE('2020-09-16') <= TIMESTAMP('2020-09-16 00:00:00'), "
        + "`ts_d_f` = TIMESTAMP('2020-09-16 10:20:30') <= DATE('1961-04-12'), "
        + "`d_ts_f` = DATE('2077-04-12') <= TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_t_t` = TIMESTAMP('" + today + " 10:20:30') <= TIME('10:20:30'), "
        + "`t_ts_t` = TIME('09:07:00') <= TIMESTAMP('3077-12-15 22:15:07'), "
        + "`ts_t_f` = TIMESTAMP('3077-09-16 10:20:30') <= TIME('09:07:00'), "
        + "`t_ts_f` = TIME('20:50:40') <= TIMESTAMP('" + today + " 10:20:30') "
        + " | fields `ts_dt_t`, `dt_ts_t`, `ts_dt_f`, `dt_ts_f`, `ts_d_t`, `d_ts_t`, `ts_d_f`,"
            + "`d_ts_f`, `ts_t_t`, `t_ts_t`, `ts_t_f`, `t_ts_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("ts_dt_t", null, "boolean"), schema("dt_ts_t", null, "boolean"),
        schema("ts_dt_f", null, "boolean"), schema("dt_ts_f", null, "boolean"),
        schema("ts_d_t", null, "boolean"), schema("d_ts_t", null, "boolean"),
        schema("ts_d_f", null, "boolean"), schema("d_ts_f", null, "boolean"),
        schema("ts_t_t", null, "boolean"), schema("t_ts_t", null, "boolean"),
        schema("ts_t_f", null, "boolean"), schema("t_ts_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLteCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`dt_ts_t` = DATETIME('2020-09-16 10:20:30') <= TIMESTAMP('2020-09-16 10:20:30'), "
        + "`ts_dt_t` = TIMESTAMP('1961-04-12 09:07:00') <= DATETIME('1984-12-15 22:15:07'), "
        + "`dt_ts_f` = DATETIME('3077-09-16 10:20:30') <= TIMESTAMP('2077-04-12 09:07:00'), "
        + "`ts_dt_f` = TIMESTAMP('2020-09-16 10:20:30') <= DATETIME('1984-12-15 22:15:07'), "
        + "`dt_d_t` = DATETIME('2020-09-16 00:00:00') <= DATE('2020-09-16'), "
        + "`d_dt_t` = DATE('1961-04-12') <= DATETIME('1984-12-15 22:15:07'), "
        + "`dt_d_f` = DATETIME('2020-09-16 10:20:30') <= DATE('1984-04-12'), "
        + "`d_dt_f` = DATE('2020-09-16') <= DATETIME('1961-04-12 09:07:00'), "
        + "`dt_t_t` = DATETIME('" + today + " 10:20:30') <= TIME('10:20:30'), "
        + "`t_dt_t` = TIME('09:07:00') <= DATETIME('3077-12-15 22:15:07'), "
        + "`dt_t_f` = DATETIME('3077-09-16 10:20:30') <= TIME('19:07:00'), "
        + "`t_dt_f` = TIME('20:40:50') <= DATETIME('" + today + " 10:20:30') "
        + " | fields `dt_ts_t`, `ts_dt_t`, `dt_ts_f`, `ts_dt_f`, `dt_d_t`, `d_dt_t`, `dt_d_f`,"
            + "`d_dt_f`, `dt_t_t`, `t_dt_t`, `dt_t_f`, `t_dt_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("dt_ts_t", null, "boolean"), schema("ts_dt_t", null, "boolean"),
        schema("dt_ts_f", null, "boolean"), schema("ts_dt_f", null, "boolean"),
        schema("dt_d_t", null, "boolean"), schema("d_dt_t", null, "boolean"),
        schema("dt_d_f", null, "boolean"), schema("d_dt_f", null, "boolean"),
        schema("dt_t_t", null, "boolean"), schema("t_dt_t", null, "boolean"),
        schema("dt_t_f", null, "boolean"), schema("t_dt_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLteCompareDateWithOtherTypes() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`d_ts_t` = DATE('2020-09-16') <= TIMESTAMP('2020-09-16 00:00:00'), "
        + "`ts_d_t` = TIMESTAMP('1961-04-12 09:07:00') <= DATE('1984-12-15'), "
        + "`d_ts_f` = DATE('2020-09-16') <= TIMESTAMP('1961-04-12 09:07:00'), "
        + "`ts_d_f` = TIMESTAMP('2077-04-12 09:07:00') <= DATE('2020-09-16'), "
        + "`d_dt_t` = DATE('2020-09-16') <= DATETIME('2020-09-16 00:00:00'), "
        + "`dt_d_t` = DATETIME('1961-04-12 10:20:30') <= DATE('1984-11-15'), "
        + "`d_dt_f` = DATE('2077-04-12') <= DATETIME('1984-12-15 22:15:07'), "
        + "`dt_d_f` = DATETIME('2020-09-16 00:00:00') <= DATE('1984-03-22'), "
        + "`d_t_t` = DATE('2020-09-16') <= TIME('09:07:00'), "
        + "`t_d_t` = TIME('09:07:00') <= DATE('3077-04-12'), "
        + "`d_t_f` = DATE('3077-04-12') <= TIME('00:00:00'), "
        + "`t_d_f` = TIME('00:00:00') <= DATE('2020-09-16') "
        + " | fields `d_ts_t`, `ts_d_t`, `d_ts_f`, `ts_d_f`, `d_dt_t`, `dt_d_t`, `d_dt_f`,"
            + " `dt_d_f`, `d_t_t`, `t_d_t`, `d_t_f`, `t_d_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("d_ts_t", null, "boolean"), schema("ts_d_t", null, "boolean"),
        schema("d_ts_f", null, "boolean"), schema("ts_d_f", null, "boolean"),
        schema("d_dt_t", null, "boolean"), schema("dt_d_t", null, "boolean"),
        schema("d_dt_f", null, "boolean"), schema("dt_d_f", null, "boolean"),
        schema("d_t_t", null, "boolean"), schema("t_d_t", null, "boolean"),
        schema("d_t_f", null, "boolean"), schema("t_d_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testLteCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`t_dt_t` = TIME('10:20:30') <= DATETIME('" + today + " 10:20:30'), "
        + "`dt_t_t` = DATETIME('" + today + " 09:07:00') <= TIME('10:20:30'), "
        + "`t_dt_f` = TIME('09:07:00') <= DATETIME('1961-04-12 09:07:00'), "
        + "`dt_t_f` = DATETIME('" + today + " 20:40:50') <= TIME('10:20:30'), "
        + "`t_ts_t` = TIME('10:20:30') <= TIMESTAMP('" + today + " 10:20:30'), "
        + "`ts_t_t` = TIMESTAMP('1984-12-15 10:20:30') <= TIME('10:20:30'), "
        + "`t_ts_f` = TIME('22:15:07') <= TIMESTAMP('1984-12-15 22:15:07'), "
        + "`ts_t_f` = TIMESTAMP('" + today + " 20:50:42') <= TIME('10:20:30'), "
        + "`t_d_t` = TIME('09:07:00') <= DATE('3077-04-12'), "
        + "`d_t_t` = DATE('2020-09-16') <= TIME('09:07:00'), "
        + "`t_d_f` = TIME('00:00:00') <= DATE('1961-04-12'), "
        + "`d_t_f` = DATE('3077-04-12') <= TIME('10:20:30') "
        + " | fields `t_dt_t`, `dt_t_t`, `t_dt_f`, `dt_t_f`, `t_ts_t`, `ts_t_t`, `t_ts_f`,"
            + " `ts_t_f`, `t_d_t`, `d_t_t`, `t_d_f`, `d_t_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("t_dt_t", null, "boolean"), schema("dt_t_t", null, "boolean"),
        schema("t_dt_f", null, "boolean"), schema("dt_t_f", null, "boolean"),
        schema("t_ts_t", null, "boolean"), schema("ts_t_t", null, "boolean"),
        schema("t_ts_f", null, "boolean"), schema("ts_t_f", null, "boolean"),
        schema("t_d_t", null, "boolean"), schema("d_t_t", null, "boolean"),
        schema("t_d_f", null, "boolean"), schema("d_t_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }
  
  
  
  @Test
  public void testGteCompareTimestampWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`ts_dt_t` = TIMESTAMP('2020-09-16 10:20:30') >= DATETIME('2020-09-16 10:20:30'), "
        + "`dt_ts_t` = DATETIME('2020-09-16 10:20:30') >= TIMESTAMP('1961-04-12 09:07:00'), "
        + "`ts_dt_f` = TIMESTAMP('2020-09-16 10:20:30') >= DATETIME('2061-04-12 09:07:00'), "
        + "`dt_ts_f` = DATETIME('1961-04-12 09:07:00') >= TIMESTAMP('1984-12-15 09:07:00'), "
        + "`ts_d_t` = TIMESTAMP('2020-09-16 10:20:30') >= DATE('1961-04-12'), "
        + "`d_ts_t` = DATE('2020-09-16') >= TIMESTAMP('2020-09-16 00:00:00'), "
        + "`ts_d_f` = TIMESTAMP('2020-09-16 10:20:30') >= DATE('2077-04-12'), "
        + "`d_ts_f` = DATE('1961-04-11') >= TIMESTAMP('1961-04-12 00:00:00'), "
        + "`ts_t_t` = TIMESTAMP('" + today + " 10:20:30') >= TIME('10:20:30'), "
        + "`t_ts_t` = TIME('20:50:40') >= TIMESTAMP('" + today + " 10:20:30'), "
        + "`ts_t_f` = TIMESTAMP('1977-07-08 10:20:30') >= TIME('10:20:30'), "
        + "`t_ts_f` = TIME('09:07:00') >= TIMESTAMP('3077-12-15 22:15:07') "
        + " | fields `ts_dt_t`, `dt_ts_t`, `ts_dt_f`, `dt_ts_f`, `ts_d_t`, `d_ts_t`, `ts_d_f`,"
            + "`d_ts_f`, `ts_t_t`, `t_ts_t`, `ts_t_f`, `t_ts_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("ts_dt_t", null, "boolean"), schema("dt_ts_t", null, "boolean"),
        schema("ts_dt_f", null, "boolean"), schema("dt_ts_f", null, "boolean"),
        schema("ts_d_t", null, "boolean"), schema("d_ts_t", null, "boolean"),
        schema("ts_d_f", null, "boolean"), schema("d_ts_f", null, "boolean"),
        schema("ts_t_t", null, "boolean"), schema("t_ts_t", null, "boolean"),
        schema("ts_t_f", null, "boolean"), schema("t_ts_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGteCompareDateTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`dt_ts_t` = DATETIME('2020-09-16 10:20:30') >= TIMESTAMP('2020-09-16 10:20:30'), "
        + "`ts_dt_t` = TIMESTAMP('2020-09-16 10:20:30') >= DATETIME('1984-12-15 22:15:07'), "
        + "`dt_ts_f` = DATETIME('2020-09-16 10:20:30') >= TIMESTAMP('2077-04-12 09:07:00'), "
        + "`ts_dt_f` = TIMESTAMP('1961-04-12 00:00:00') >= DATETIME('1961-04-12 09:07:00'), "
        + "`dt_d_t` = DATETIME('2020-09-16 00:00:00') >= DATE('2020-09-16'), "
        + "`d_dt_t` = DATE('2020-09-16') >= DATETIME('1961-04-12 09:07:00'), "
        + "`dt_d_f` = DATETIME('1961-04-12 09:07:00') >= DATE('2020-09-16'), "
        + "`d_dt_f` = DATE('1961-04-12') >= DATETIME('1984-12-15 22:15:07'), "
        + "`dt_t_t` = DATETIME('" + today + " 10:20:30') >= TIME('10:20:30'), "
        + "`t_dt_t` = TIME('20:40:50') >= DATETIME('" + today + " 10:20:30'), "
        + "`dt_t_f` = DATETIME('1961-04-12 09:07:00') >= TIME('09:07:00'), "
        + "`t_dt_f` = TIME('09:07:00') >= DATETIME('3077-12-15 22:15:07') "
        + " | fields `dt_ts_t`, `ts_dt_t`, `dt_ts_f`, `ts_dt_f`, `dt_d_t`, `d_dt_t`, `dt_d_f`,"
            + "`d_dt_f`, `dt_t_t`, `t_dt_t`, `dt_t_f`, `t_dt_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("dt_ts_t", null, "boolean"), schema("ts_dt_t", null, "boolean"),
        schema("dt_ts_f", null, "boolean"), schema("ts_dt_f", null, "boolean"),
        schema("dt_d_t", null, "boolean"), schema("d_dt_t", null, "boolean"),
        schema("dt_d_f", null, "boolean"), schema("d_dt_f", null, "boolean"),
        schema("dt_t_t", null, "boolean"), schema("t_dt_t", null, "boolean"),
        schema("dt_t_f", null, "boolean"), schema("t_dt_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGteCompareDateWithOtherTypes() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`d_ts_t` = DATE('2020-09-16') >= TIMESTAMP('2020-09-16 00:00:00'), "
        + "`ts_d_t` = TIMESTAMP('2077-04-12 09:07:00') >= DATE('2020-09-16'), "
        + "`d_ts_f` = DATE('1961-04-12') >= TIMESTAMP('1961-04-12 09:07:00'), "
        + "`ts_d_f` = TIMESTAMP('1961-04-12 09:07:00') >= DATE('1984-12-15'), "
        + "`d_dt_t` = DATE('2020-09-16') >= DATETIME('2020-09-16 00:00:00'), "
        + "`dt_d_t` = DATETIME('2020-09-16 00:00:00') >= DATE('1984-03-22'), "
        + "`d_dt_f` = DATE('1960-12-15') >= DATETIME('1961-04-12 09:07:00'), "
        + "`dt_d_f` = DATETIME('1961-04-12 10:20:30') >= DATE('1984-11-15'), "
        + "`d_t_t` = DATE('3077-04-12') >= TIME('00:00:00'), "
        + "`t_d_t` = TIME('00:00:00') >= DATE('2020-09-16'), "
        + "`d_t_f` = DATE('2020-09-16') >= TIME('09:07:00'), "
        + "`t_d_f` = TIME('09:07:00') >= DATE('3077-04-12') "
        + " | fields `d_ts_t`, `ts_d_t`, `d_ts_f`, `ts_d_f`, `d_dt_t`, `dt_d_t`, `d_dt_f`,"
            + " `dt_d_f`, `d_t_t`, `t_d_t`, `d_t_f`, `t_d_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("d_ts_t", null, "boolean"), schema("ts_d_t", null, "boolean"),
        schema("d_ts_f", null, "boolean"), schema("ts_d_f", null, "boolean"),
        schema("d_dt_t", null, "boolean"), schema("dt_d_t", null, "boolean"),
        schema("d_dt_f", null, "boolean"), schema("dt_d_f", null, "boolean"),
        schema("d_t_t", null, "boolean"), schema("t_d_t", null, "boolean"),
        schema("d_t_f", null, "boolean"), schema("t_d_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }

  @Test
  public void testGteCompareTimeWithOtherTypes() throws IOException {
    var today = LocalDate.now().toString();
    JSONObject result = executeQuery(String.format("source=%s | eval "
        + "`t_dt_t` = TIME('10:20:30') >= DATETIME('" + today + " 10:20:30'), "
        + "`dt_t_t` = DATETIME('" + today + " 20:40:50') >= TIME('10:20:30'), "
        + "`t_dt_f` = TIME('09:07:00') >= DATETIME('3077-04-12 09:07:00'), "
        + "`dt_t_f` = DATETIME('" + today + " 09:07:00') >= TIME('10:20:30'), "
        + "`t_ts_t` = TIME('10:20:30') >= TIMESTAMP('" + today + " 10:20:30'), "
        + "`ts_t_t` = TIMESTAMP('" + today + " 20:50:42') >= TIME('10:20:30'), "
        + "`t_ts_f` = TIME('22:15:07') >= TIMESTAMP('3077-12-15 22:15:07'), "
        + "`ts_t_f` = TIMESTAMP('1984-12-15 10:20:30') >= TIME('10:20:30'), "
        + "`t_d_t` = TIME('00:00:00') >= DATE('1961-04-12'), "
        + "`d_t_t` = DATE('3077-04-12') >= TIME('10:20:30'), "
        + "`t_d_f` = TIME('09:07:00') >= DATE('3077-04-12'), "
        + "`d_t_f` = DATE('2020-09-16') >= TIME('09:07:00') "
        + " | fields `t_dt_t`, `dt_t_t`, `t_dt_f`, `dt_t_f`, `t_ts_t`, `ts_t_t`, `t_ts_f`,"
            + " `ts_t_f`, `t_d_t`, `d_t_t`, `t_d_f`, `d_t_f`", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("t_dt_t", null, "boolean"), schema("dt_t_t", null, "boolean"),
        schema("t_dt_f", null, "boolean"), schema("dt_t_f", null, "boolean"),
        schema("t_ts_t", null, "boolean"), schema("ts_t_t", null, "boolean"),
        schema("t_ts_f", null, "boolean"), schema("ts_t_f", null, "boolean"),
        schema("t_d_t", null, "boolean"), schema("d_t_t", null, "boolean"),
        schema("t_d_f", null, "boolean"), schema("d_t_f", null, "boolean"));
    verifyDataRows(result,
        rows(true, true, false, false, true, true, false, false, true, true, false, false));
  }
}
