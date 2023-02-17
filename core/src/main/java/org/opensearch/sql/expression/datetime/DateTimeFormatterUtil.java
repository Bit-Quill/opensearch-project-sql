/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import com.google.common.collect.ImmutableMap;

import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.math3.analysis.function.Exp;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;

/**
 * This class converts a SQL style DATE_FORMAT format specifier and converts it to a
 * Java SimpleDateTime format.
 */
class DateTimeFormatterUtil {
  private static final int SUFFIX_SPECIAL_START_TH = 11;
  private static final int SUFFIX_SPECIAL_END_TH = 13;
  private static final String SUFFIX_SPECIAL_TH = "th";

  private static final String NANO_SEC_FORMAT = "'%06d'";

  private static final int DEFAULT_YEAR_VAL = 2000;

  private static final int DEFAULT_MONTH_VAL = 1;

  private static final int DEFAULT_DAY_VAL = 1;

  private static final int DEFAULT_HOUR_VAL = 0;

  private static final int DEFAULT_MINUTE_VAL = 0;

  private static final int DEFAULT_SECOND_VAL = 0;


  private static String[] DATE_SPECIFIERS = {
      "%a", //Abbreviated weekday name (Sun..Sat)
      "%b", // Abbreviated month name (Jan..Dec)
      "%c", // Month, numeric (0..12)
      "%d", // Day of the month, numeric (00..31)
      "%e", // Day of the month, numeric (0..31)
      "%j", // Day of the year, numeric (001..366)
      "%M", //  Month name (January..December)
      "%m", //  Month, numeric (00..12)
      "%W", //  Weekday name (Sunday..Saturday)
      "%Y", //  Year, numeric, 4 digits
      "%y", // Year, numeric, 2 digits
      "%D", // Day of month with English suffix
      "%w", // Day of week (0 indexed)
      "%U", // Week where Sunday is the first day - WEEK() mode 0
      "%u", // Week where Monday is the first day - WEEK() mode 1
      "%V", // Week where Sunday is the first day - WEEK() mode 2 used with %X
      "%v", // Week where Monday is the first day - WEEK() mode 3 used with %x
      "%X", // Year for week where Sunday is the first day, 4 digits used with %V
      "%x" // Year for week where Monday is the first day, 4 digits used with %v};
  };

  private static String[] TIME_SPECIFIERS = {
      "%H", // Hour (00..23)
      "%h", // Hour (01..12)
      "%I", // Hour (01..12)
      "%i", // Minutes, numeric (00..59)
      "%k", // Hour (0..23)
      "%l", // hour (1..12)
      "%p", // AM or PM
      "%r", // hh:mm:ss followed by AM or PM
      "%S", // Seconds (00..59)
      "%s", // Seconds (00..59)
      "%T", // HH:mm:ss
      "%f", // Microseconds
  };

  private static final Map<Integer, String> SUFFIX_CONVERTER =
      ImmutableMap.<Integer, String>builder()
      .put(1, "st").put(2, "nd").put(3, "rd").build();

  // The following have special cases that need handling outside of the format options provided
  // by the DateTimeFormatter class.
  interface DateTimeFormatHandler {
    String getFormat(LocalDateTime date);
  }

  interface StrToDateHandler {
    String getFormat();
  }

  private static final Map<String, DateTimeFormatHandler> DATE_HANDLERS =
      ImmutableMap.<String, DateTimeFormatHandler>builder()
      .put("%a", (date) -> "EEE") // %a => EEE - Abbreviated weekday name (Sun..Sat)
      .put("%b", (date) -> "LLL") // %b => LLL - Abbreviated month name (Jan..Dec)
      .put("%c", (date) -> "MM") // %c => MM - Month, numeric (0..12)
      .put("%d", (date) -> "dd") // %d => dd - Day of the month, numeric (00..31)
      .put("%e", (date) -> "d") // %e => d - Day of the month, numeric (0..31)
      .put("%H", (date) -> "HH") // %H => HH - (00..23)
      .put("%h", (date) -> "hh") // %h => hh - (01..12)
      .put("%I", (date) -> "hh") // %I => hh - (01..12)
      .put("%i", (date) -> "mm") // %i => mm - Minutes, numeric (00..59)
      .put("%j", (date) -> "DDD") // %j => DDD - (001..366)
      .put("%k", (date) -> "H") // %k => H - (0..23)
      .put("%l", (date) -> "h") // %l => h - (1..12)
      .put("%p", (date) -> "a") // %p => a - AM or PM
      .put("%M", (date) -> "LLLL") // %M => LLLL - Month name (January..December)
      .put("%m", (date) -> "MM") // %m => MM - Month, numeric (00..12)
      .put("%r", (date) -> "hh:mm:ss a") // %r => hh:mm:ss a - hh:mm:ss followed by AM or PM
      .put("%S", (date) -> "ss") // %S => ss - Seconds (00..59)
      .put("%s", (date) -> "ss") // %s => ss - Seconds (00..59)
      .put("%T", (date) -> "HH:mm:ss") // %T => HH:mm:ss
      .put("%W", (date) -> "EEEE") // %W => EEEE - Weekday name (Sunday..Saturday)
      .put("%Y", (date) -> "yyyy") // %Y => yyyy - Year, numeric, 4 digits
      .put("%y", (date) -> "yy") // %y => yy - Year, numeric, 2 digits
      // The following are not directly supported by DateTimeFormatter.
      .put("%D", (date) -> // %w - Day of month with English suffix
          String.format("'%d%s'", date.getDayOfMonth(), getSuffix(date.getDayOfMonth())))
      .put("%f", (date) -> // %f - Microseconds
          String.format(NANO_SEC_FORMAT, (date.getNano() / 1000)))
      .put("%w", (date) -> // %w - Day of week (0 indexed)
          String.format("'%d'", date.getDayOfWeek().getValue()))
      .put("%U", (date) -> // %U Week where Sunday is the first day - WEEK() mode 0
          String.format("'%d'", CalendarLookup.getWeekNumber(0, date.toLocalDate())))
      .put("%u", (date) -> // %u Week where Monday is the first day - WEEK() mode 1
          String.format("'%d'", CalendarLookup.getWeekNumber(1, date.toLocalDate())))
      .put("%V", (date) -> // %V Week where Sunday is the first day - WEEK() mode 2 used with %X
          String.format("'%d'", CalendarLookup.getWeekNumber(2, date.toLocalDate())))
      .put("%v", (date) -> // %v Week where Monday is the first day - WEEK() mode 3 used with %x
          String.format("'%d'", CalendarLookup.getWeekNumber(3, date.toLocalDate())))
      .put("%X", (date) -> // %X Year for week where Sunday is the first day, 4 digits used with %V
          String.format("'%d'", CalendarLookup.getYearNumber(2, date.toLocalDate())))
      .put("%x", (date) -> // %x Year for week where Monday is the first day, 4 digits used with %v
          String.format("'%d'", CalendarLookup.getYearNumber(3, date.toLocalDate())))
      .build();

  //Handlers for the time_format function.
  //Some format specifiers return 0 or null to align with MySQL.
  //https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_time-format
  private static final Map<String, DateTimeFormatHandler> TIME_HANDLERS =
      ImmutableMap.<String, DateTimeFormatHandler>builder()
          .put("%a", (date) -> null)
          .put("%b", (date) -> null)
          .put("%c", (date) -> "0")
          .put("%d", (date) -> "00")
          .put("%e", (date) -> "0")
          .put("%H", (date) -> "HH") // %H => HH - (00..23)
          .put("%h", (date) -> "hh") // %h => hh - (01..12)
          .put("%I", (date) -> "hh") // %I => hh - (01..12)
          .put("%i", (date) -> "mm") // %i => mm - Minutes, numeric (00..59)
          .put("%j", (date) -> null)
          .put("%k", (date) -> "H") // %k => H - (0..23)
          .put("%l", (date) -> "h") // %l => h - (1..12)
          .put("%p", (date) -> "a") // %p => a - AM or PM
          .put("%M", (date) -> null)
          .put("%m", (date) -> "00")
          .put("%r", (date) -> "hh:mm:ss a") // %r => hh:mm:ss a - hh:mm:ss followed by AM or PM
          .put("%S", (date) -> "ss") // %S => ss - Seconds (00..59)
          .put("%s", (date) -> "ss") // %s => ss - Seconds (00..59)
          .put("%T", (date) -> "HH:mm:ss") // %T => HH:mm:ss
          .put("%W", (date) -> null)
          .put("%Y", (date) -> "0000")
          .put("%y", (date) -> "00")
          .put("%D", (date) -> null)
          .put("%f", (date) -> // %f - Microseconds
              String.format(NANO_SEC_FORMAT, (date.getNano() / 1000)))
          .put("%w", (date) -> null)
          .put("%U", (date) -> null)
          .put("%u", (date) -> null)
          .put("%V", (date) -> null)
          .put("%v", (date) -> null)
          .put("%X", (date) -> null)
          .put("%x", (date) -> null)
          .build();

  private static final Map<String, StrToDateHandler> STR_TO_DATE_HANDLERS =
      ImmutableMap.<String, StrToDateHandler>builder()
          .put("%a", () -> "EEE") // %a => EEE - Abbreviated weekday name (Sun..Sat)
          .put("%b", () -> "LLL") // %b => LLL - Abbreviated month name (Jan..Dec)
          .put("%c", () -> "M") // %c => MM - Month, numeric (0..12)
          .put("%d", () -> "d") // %d => dd - Day of the month, numeric (00..31)
          .put("%e", () -> "d") // %e => d - Day of the month, numeric (0..31)
          .put("%H", () -> "H") // %H => HH - (00..23)
          .put("%h", () -> "H") // %h => hh - (01..12)
          .put("%I", () -> "h") // %I => hh - (01..12)
          .put("%i", () -> "m") // %i => mm - Minutes, numeric (00..59)
          .put("%j", () -> "DDD") // %j => DDD - (001..366)
          .put("%k", () -> "H") // %k => H - (0..23)
          .put("%l", () -> "h") // %l => h - (1..12)
          .put("%p", () -> "a") // %p => a - AM or PM
          .put("%M", () -> "LLLL") // %M => LLLL - Month name (January..December)
          .put("%m", () -> "M") // %m => MM - Month, numeric (00..12)
          .put("%r", () -> "hh:mm:ss a") // %r => hh:mm:ss a - hh:mm:ss followed by AM or PM
          .put("%S", () -> "s") // %S => ss - Seconds (00..59)
          .put("%s", () -> "s") // %s => ss - Seconds (00..59)
          .put("%T", () -> "HH:mm:ss") // %T => HH:mm:ss
          .put("%W", () -> "EEEE") // %W => EEEE - Weekday name (Sunday..Saturday)
          .put("%Y", () -> "y") // %Y => yyyy - Year, numeric, 4 digits
          .put("%y", () -> "u") // %y => yy - Year, numeric, 2 digits
          .put("%f", () -> "n") // %f => n - Nanoseconds
          //The following have been implemented but cannot be aligned with
          // MySQL due to the limitations of the DatetimeFormatter
          .put("%D", () -> "d") // %w - Day of month with English suffix
          .put("%w", () -> "e") // %w - Day of week (0 indexed)
          .put("%U", () -> "w") // %U Week where Sunday is the first day - WEEK() mode 0
          .put("%u", () -> "w") // %u Week where Monday is the first day - WEEK() mode 1
          .put("%V", () -> "w") // %V Week where Sunday is the first day - WEEK() mode 2 used with %X
          .put("%v", () -> "w") // %v Week where Monday is the first day - WEEK() mode 3 used with %x
          .put("%X", () -> "yyyy") // %X Year for week where Sunday is the first day, 4 digits used with %V
          .put("%x", () -> "yyyy" )// %x Year for week where Monday is the first day, 4 digits used with %v
          .build();

  private static final Pattern pattern = Pattern.compile("%.");
  private static final Pattern CHARACTERS_WITH_NO_MOD_LITERAL_BEHIND_PATTERN
          = Pattern.compile("(?<!%)[a-zA-Z&&[^aydmshiHIMYDSEL]]+");
  private static final String MOD_LITERAL = "%";

  private DateTimeFormatterUtil() {
  }

  static StringBuffer getCleanFormat(ExprValue formatExpr) {
    final StringBuffer cleanFormat = new StringBuffer();
    final Matcher m = CHARACTERS_WITH_NO_MOD_LITERAL_BEHIND_PATTERN
        .matcher(formatExpr.stringValue());

    while (m.find()) {
      m.appendReplacement(cleanFormat,String.format("'%s'", m.group()));
    }
    m.appendTail(cleanFormat);

    return cleanFormat;
  }

  /**
   * Helper function to format a DATETIME according to a provided handler and matcher.
   * @param formatExpr ExprValue containing the format expression
   * @param handler Map of character patterns to their associated datetime format
   * @param datetime The datetime argument being formatted
   * @return A formatted string expression
   */
  static ExprValue getFormattedString(ExprValue formatExpr,
                                      Map<String, DateTimeFormatHandler> handler,
                                      LocalDateTime datetime) {
    StringBuffer cleanFormat = getCleanFormat(formatExpr);

    final Matcher matcher = pattern.matcher(cleanFormat.toString());
    final StringBuffer format = new StringBuffer();
    try {
      while (matcher.find()) {
        matcher.appendReplacement(format,
            handler.getOrDefault(matcher.group(), (d) ->
                    String.format("'%s'", matcher.group().replaceFirst(MOD_LITERAL, "")))
                .getFormat(datetime));
      }
    } catch (Exception e) {
      return ExprNullValue.of();
    }
    matcher.appendTail(format);

    // English Locale matches SQL requirements.
    // 'AM'/'PM' instead of 'a.m.'/'p.m.'
    // 'Sat' instead of 'Sat.' etc
    return new ExprStringValue(datetime.format(
        DateTimeFormatter.ofPattern(format.toString(), Locale.ENGLISH)));
  }

  /**
   * Format the date using the date format String.
   * @param dateExpr the date ExprValue of Date/Datetime/Timestamp/String type.
   * @param formatExpr the format ExprValue of String type.
   * @return Date formatted using format and returned as a String.
   */
  static ExprValue getFormattedDate(ExprValue dateExpr, ExprValue formatExpr) {
    final LocalDateTime date = dateExpr.datetimeValue();
    return getFormattedString(formatExpr, DATE_HANDLERS, date);
  }

  static ExprValue getFormattedDateOfToday(ExprValue formatExpr, ExprValue time, Clock current) {
    final LocalDateTime date = LocalDateTime.of(LocalDate.now(current), time.timeValue());

    return getFormattedString(formatExpr, DATE_HANDLERS, date);
  }

  /**
   * Format the date using the date format String.
   * @param timeExpr the date ExprValue of Date/Datetime/Timestamp/String type.
   * @param formatExpr the format ExprValue of String type.
   * @return Date formatted using format and returned as a String.
   */
  static ExprValue getFormattedTime(ExprValue timeExpr, ExprValue formatExpr) {
    //Initializes DateTime with LocalDate.now(). This is safe because the date is ignored.
    //The time_format function will only return 0 or null for invalid string format specifiers.
    final LocalDateTime time = LocalDateTime.of(LocalDate.now(), timeExpr.timeValue());

    return getFormattedString(formatExpr, TIME_HANDLERS, time);
  }

  private static ExprCoreType getReturnType(ExprValue formatStringExpr) {
    String formatString = formatStringExpr.stringValue();

    boolean hasDate = false;
    boolean hasTime = false;

    for (String dateSpecifier: DATE_SPECIFIERS) {
      if (formatString.contains(dateSpecifier)){
        hasDate = true;
      }
    }
    for (String timeSpecifier: TIME_SPECIFIERS) {
      if (formatString.contains(timeSpecifier)){
        hasTime = true;
      }
    }

    if (hasDate && hasTime){
      return ExprCoreType.DATETIME;
    } else if (hasTime) {
      return ExprCoreType.TIME;
    }

    return ExprCoreType.DATE;
  }

  static ExprValue parseStringWithDateOrTime(ExprValue datetimeStringExpr,
                                       ExprValue formatExpr) {
    StringBuffer cleanFormat = getCleanFormat(formatExpr);

    final Matcher matcher = pattern.matcher(cleanFormat.toString());
    final StringBuffer format = new StringBuffer();
    try {
      while (matcher.find()) {
        matcher.appendReplacement(format,
            STR_TO_DATE_HANDLERS.getOrDefault(matcher.group(), () ->
                    String.format("'%s'", matcher.group().replaceFirst(MOD_LITERAL, "")))
                .getFormat());
      }
    } catch (Exception e) {
      return ExprNullValue.of();
    }
    matcher.appendTail(format);

    DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern(format.toString())
        .toFormatter();

    TemporalAccessor ta = formatter.parse(datetimeStringExpr.stringValue());

    switch (getReturnType(formatExpr)) {
      case DATETIME:
        LocalDateTime datetime = LocalDateTime.from(ta);
        return new ExprDatetimeValue(datetime);
      case TIME:
        LocalTime time = LocalTime.from(ta);
        return new ExprTimeValue(time);
      default:
        LocalDate date = LocalDate.from(ta);
        return new ExprDateValue(date);
    }

  }


  /**
   * Returns English suffix of incoming value.
   * @param val Incoming value.
   * @return English suffix as String (st, nd, rd, th)
   */
  private static String getSuffix(int val) {
    // The numbers 11, 12, and 13 do not follow general suffix rules.
    if ((SUFFIX_SPECIAL_START_TH <= val) && (val <= SUFFIX_SPECIAL_END_TH)) {
      return SUFFIX_SPECIAL_TH;
    }
    return SUFFIX_CONVERTER.getOrDefault(val % 10, SUFFIX_SPECIAL_TH);
  }
}
