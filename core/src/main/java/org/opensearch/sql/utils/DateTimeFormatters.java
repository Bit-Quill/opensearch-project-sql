/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.utils;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

//import org.opensearch.common.time.DateFormatter;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;

/**
 * DateTimeFormatter.
 * Reference org.opensearch.common.time.DateFormatters.
 */
@UtilityClass
public class DateTimeFormatters {

  //Length of a date formatted as YYYYMMDD.
  public static final int FULL_DATE_LENGTH = 8;

  //Length of a date formatted as YYMMDD.
  public static final int SHORT_DATE_LENGTH = 6;

  //Length of a date formatted as YMMDD.
  public static final int SINGLE_DIGIT_YEAR_DATE_LENGTH = 5;

  //Length of a date formatted as MMDD.
  public static final int NO_YEAR_DATE_LENGTH = 4;

  //Length of a date formatted as MDD.
  public static final int SINGLE_DIGIT_MONTH_DATE_LENGTH = 3;

  public static final DateTimeFormatter TIME_ZONE_FORMATTER_NO_COLON =
      new DateTimeFormatterBuilder()
          .appendOffset("+HHmm", "Z")
          .toFormatter(Locale.ROOT)
          .withResolverStyle(ResolverStyle.STRICT);

  public static final DateTimeFormatter STRICT_YEAR_MONTH_DAY_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
          .optionalStart()
          .appendLiteral("-")
          .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
          .optionalStart()
          .appendLiteral('-')
          .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
          .optionalEnd()
          .optionalEnd()
          .toFormatter(Locale.ROOT)
          .withResolverStyle(ResolverStyle.STRICT);

  public static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
          .appendLiteral(':')
          .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
          .appendLiteral(':')
          .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
          .toFormatter(Locale.ROOT)
          .withResolverStyle(ResolverStyle.STRICT);

  public static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
          .optionalStart()
          .appendLiteral('T')
          .optionalStart()
          .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
          .optionalStart()
          .appendLiteral(':')
          .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
          .optionalStart()
          .appendLiteral(':')
          .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
          .optionalStart()
          .appendFraction(NANO_OF_SECOND, 1, 9, true)
          .optionalEnd()
          .optionalStart()
          .appendLiteral(',')
          .appendFraction(NANO_OF_SECOND, 1, 9, false)
          .optionalEnd()
          .optionalEnd()
          .optionalEnd()
          .optionalStart()
          .appendZoneOrOffsetId()
          .optionalEnd()
          .optionalStart()
          .append(TIME_ZONE_FORMATTER_NO_COLON)
          .optionalEnd()
          .optionalEnd()
          .optionalEnd()
          .toFormatter(Locale.ROOT)
          .withResolverStyle(ResolverStyle.STRICT);

  public static final DateTimeFormatter SQL_LITERAL_DATE_TIME_FORMAT = DateTimeFormatter
          .ofPattern("yyyy-MM-dd HH:mm:ss");

  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendOptional(SQL_LITERAL_DATE_TIME_FORMAT)
          .appendOptional(STRICT_DATE_OPTIONAL_TIME_FORMATTER)
          .appendOptional(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
          .toFormatter();

  /**
   * todo. only support timestamp in format yyyy-MM-dd HH:mm:ss.
   */
  public static final DateTimeFormatter DATE_TIME_FORMATTER_WITHOUT_NANO =
      SQL_LITERAL_DATE_TIME_FORMAT;

  private static final int MIN_FRACTION_SECONDS = 0;
  private static final int MAX_FRACTION_SECONDS = 9;

  public static final DateTimeFormatter DATE_TIME_FORMATTER_VARIABLE_NANOS =
      new DateTimeFormatterBuilder()
          .appendPattern("uuuu-MM-dd HH:mm:ss")
          .appendFraction(
              ChronoField.NANO_OF_SECOND,
              MIN_FRACTION_SECONDS,
              MAX_FRACTION_SECONDS,
              true)
          .toFormatter(Locale.ROOT)
          .withResolverStyle(ResolverStyle.STRICT);

  public static final DateTimeFormatter DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL =
      new DateTimeFormatterBuilder()
          .appendPattern("[uuuu-MM-dd HH:mm:ss][uuuu-MM-dd HH:mm][HH:mm:ss][HH:mm][uuuu-MM-dd]")
          .appendFraction(
              ChronoField.NANO_OF_SECOND,
              MIN_FRACTION_SECONDS,
              MAX_FRACTION_SECONDS,
              true)
          .toFormatter(Locale.ROOT)
          .withResolverStyle(ResolverStyle.STRICT);

  // MDD
  public static final DateTimeFormatter DATE_FORMATTER_SINGLE_DIGIT_MONTH =
      new DateTimeFormatterBuilder()
          .parseDefaulting(YEAR, 2000)
          .appendPattern("Mdd")
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  // MMDD
  public static final DateTimeFormatter DATE_FORMATTER_NO_YEAR =
      new DateTimeFormatterBuilder()
          .parseDefaulting(YEAR, 2000)
          .appendPattern("MMdd")
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  // YMMDD
  public static final DateTimeFormatter DATE_FORMATTER_SINGLE_DIGIT_YEAR =
      new DateTimeFormatterBuilder()
          .appendValueReduced(YEAR, 1, 1, 2000)
          .appendPattern("MMdd")
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  // YYMMDD
  public static final DateTimeFormatter DATE_FORMATTER_SHORT_YEAR =
      new DateTimeFormatterBuilder()
          .appendValueReduced(YEAR, 2, 2, 1970)
          .appendPattern("MMdd")
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  // YYYYMMDD
  public static final DateTimeFormatter DATE_FORMATTER_LONG_YEAR =
      new DateTimeFormatterBuilder()
          .appendValue(YEAR, 4)
          .appendPattern("MMdd")
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  // YYMMDDhhmmss
  public static final DateTimeFormatter DATE_TIME_FORMATTER_SHORT_YEAR =
      new DateTimeFormatterBuilder()
          .appendValueReduced(YEAR, 2, 2, 1970)
          .appendPattern("MMddHHmmss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  // YYYYMMDDhhmmss
  public static final DateTimeFormatter DATE_TIME_FORMATTER_LONG_YEAR =
      new DateTimeFormatterBuilder()
          .appendValue(YEAR,4)
          .appendPattern("MMddHHmmss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  public static final DateTimeFormatter DATE_TIME_FORMATTER_STRICT_WITH_TZ =
      new DateTimeFormatterBuilder()
          .appendPattern("uuuu-MM-dd HH:mm:ss[xxx]")
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  //Formatters below implement the standard/named datetime formatters available for OpenSearchSQL

  //epoch_millis
  public static final DateTimeFormatter EPOCH_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendValue(ChronoField.INSTANT_SECONDS, 1, 19, SignStyle.NEVER)
          .appendValue(ChronoField.MILLI_OF_SECOND, 3)
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //epoch_second
  public static final DateTimeFormatter EPOCH_SECOND_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //date_optional_time or strict_date_optional_time
  public static final DateTimeFormatter DATE_OPTIONAL_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //strict_date_optional_time_nanos
  public static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_NANOS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_date
  public static final DateTimeFormatter BASIC_DATE_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_date_time
  public static final DateTimeFormatter BASIC_DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_date_time_no_millis
  public static final DateTimeFormatter BASIC_DATE_TIME_NO_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_ordinal_date
  public static final DateTimeFormatter BASIC_ORDINAL_DATE_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_ordinal_date_time
  public static final DateTimeFormatter BASIC_ORDINAL_DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_ordinal_date_time_no_millis
  public static final DateTimeFormatter BASIC_ORDINAL_DATE_TIME_NO_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_time
  public static final DateTimeFormatter BASIC_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_time_no_millis
  public static final DateTimeFormatter BASIC_TIME_NO_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_t_time
  public static final DateTimeFormatter BASIC_T_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_t_time_no_millis
  public static final DateTimeFormatter BASIC_T_TIME_NO_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_week_date or strict_basic_week_date
  public static final DateTimeFormatter BASIC_WEEK_DATE_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_week_date_time or strict_basic_week_date_time
  public static final DateTimeFormatter BASIC_WEEK_DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //basic_week_date_time_no_millis or strict_basic_week_date_time_no_millis

  public static final DateTimeFormatter BASIC_WEEK_DATE_TIME_NO_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //date or strict_date
  public static final DateTimeFormatter DATE_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //date_hour or strict_date_hour
  public static final DateTimeFormatter DATE_HOUR_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //date_hour_minute or strict_date_hour_minute
  public static final DateTimeFormatter DATE_HOUR_MINUTE_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //date_hour_minute_second or strict_date_hour_minute_second
  public static final DateTimeFormatter DATE_HOUR_MINUTE_SECOND_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //date_hour_minute_second_fraction or strict_date_hour_minute_second_fraction
  public static final DateTimeFormatter DATE_HOUR_MINUTE_SECOND_FRACTION_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //date_hour_minute_second_millis or strict_date_hour_minute_second_millis
  public static final DateTimeFormatter DATE_HOUR_MINUTE_SECOND_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
//  //date_time or strict_date_time
//  public static final DateTimeFormatter DATE_TIME_FORMATTER =
//      new DateTimeFormatterBuilder()
//          .appendPattern("ss")
//          .toFormatter()
//          .withResolverStyle(ResolverStyle.SMART);
  //date_time_no_millis or strict_date_time_no_millis
  public static final DateTimeFormatter DATE_TIME_NO_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //hour or strict_hour
  public static final DateTimeFormatter HOUR_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //hour_minute or strict_hour_minute
  public static final DateTimeFormatter HOUR_MINUTE_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //hour_minute_second or strict_hour_minute_second
  public static final DateTimeFormatter HOUR_MINUTE_SECOND_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //hour_minute_second_fraction or strict_hour_minute_second_fraction
  public static final DateTimeFormatter HOUR_MINUTE_SECOND_FRACTION_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //hour_minute_second_millis or strict_hour_minute_second_millis
  public static final DateTimeFormatter HOUR_MINUTE_SECOND_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //ordinal_date or strict_ordinal_date
  public static final DateTimeFormatter ORDINAL_DATE_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //ordinal_date_time or strict_ordinal_date_time
  public static final DateTimeFormatter ORDINAL_DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //ordinal_date_time_no_millis or strict_ordinal_date_time_no_millis
  public static final DateTimeFormatter ORDINAL_DATE_TIME_NO_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //time or strict_time
  public static final DateTimeFormatter TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //time_no_millis or strict_time_no_millis
  public static final DateTimeFormatter TIME_NO_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //t_time or strict_t_time
  public static final DateTimeFormatter T_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //t_time_no_millis or strict_t_time_no_millis
  public static final DateTimeFormatter T_TIME_NO_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //week_date or strict_week_date
  public static final DateTimeFormatter WEEK_DATE_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //week_date_time or strict_week_date_time
  public static final DateTimeFormatter WEEK_DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //week_date_time_no_millis or strict_week_date_time_no_millis
  public static final DateTimeFormatter WEEK_DATE_TIME_NO_MILLIS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //weekyear or strict_weekyear
  public static final DateTimeFormatter WEEKYEAR_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //weekyear_week or strict_weekyear_week
  public static final DateTimeFormatter WEEKYEAR_WEEK_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //weekyear_week_day or strict_weekyear_week_day
  public static final DateTimeFormatter WEEKYEAR_WEEK_DAY_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //year or strict_year
  public static final DateTimeFormatter YEAR_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //year_month or strict_year_month
  public static final DateTimeFormatter YEAR_MONTH_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);
  //year_month_day or strict_year_month_day
  public static final DateTimeFormatter YEAR_MONTH_DAY_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("ss")
          .toFormatter()
          .withResolverStyle(ResolverStyle.SMART);

//  public final String formats = "epoch_millis||epoch_second";
//
//  public List<String> getFormatList() {
//    if (formats == null || formats.isEmpty()) {
//      return List.of();
//    }
//    return Arrays.stream(formats.split("\\|\\|")).map(String::trim).collect(Collectors.toList());
//  }

//  public List<DateTimeFormatter> getNamedFormatters() {
//    return getFormatList().stream().filter(f -> {
//          try {
//            DateTimeFormatter.ofPattern(f);
//            return false;
//          } catch (Exception e) {
//            return true;
//          }
//        })
//        .map(DateFormatter::forPattern).collect(Collectors.toList());
//  }

  public String getFormatter(String format) {
    switch(format) {
      case "epoch_second":
        return "SSS";
      default:
        return null;
    }
  }
}
