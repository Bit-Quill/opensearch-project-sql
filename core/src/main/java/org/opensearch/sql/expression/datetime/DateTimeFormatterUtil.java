/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import com.google.common.collect.ImmutableMap;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;

/**
 * This class converts a SQL style DATE_FORMAT format specifier and converts it to a
 * Java SimpleDateTime format.
 */
class DateTimeFormatterUtil {
  private static final int SUFFIX_SPECIAL_START_TH = 11;
  private static final int SUFFIX_SPECIAL_END_TH = 13;
  private static final String SUFFIX_SPECIAL_TH = "th";

  private static final int NANO_SEC_STRING_LENGTH = 6;
  private static final Map<Integer, String> SUFFIX_CONVERTER =
      ImmutableMap.<Integer, String>builder()
      .put(1, "st").put(2, "nd").put(3, "rd").build();

  // The following have special cases that need handling outside of the format options provided
  // by the DateTimeFormatter class.
  interface DateTimeFormatHandler {
    String getFormat(LocalDateTime date);
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
          String.format("'%d'", (date.getNano() / 1000)))
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

  private static final Map<String, DateTimeFormatHandler> TIME_HANDLERS =
      ImmutableMap.<String, DateTimeFormatHandler>builder()
          .put("%H", (date) -> "HH") // %H => HH - (00..23)
          .put("%h", (date) -> "hh") // %h => hh - (01..12)
          .put("%I", (date) -> "hh") // %I => hh - (01..12)
          .put("%i", (date) -> "mm") // %i => mm - Minutes, numeric (00..59)
          .put("%p", (date) -> "a") // %p => a - AM or PM
          .put("%r", (date) -> "hh:mm:ss a") // %r => hh:mm:ss a - hh:mm:ss followed by AM or PM
          .put("%S", (date) -> "ss") // %S => ss - Seconds (00..59)
          .put("%s", (date) -> "ss") // %s => ss - Seconds (00..59)
          .put("%T", (date) -> "HH:mm:ss") // %T => HH:mm:ss
          // The following are not directly supported by DateTimeFormatter.
          .put("%f", (date) -> // %f - Microseconds
              String.format("'%s'", formatNanoseconds((date.getNano() / 1000))))
          .build();

  private static String formatNanoseconds(int nanosecondsVal) {
    String nanoseconds = String.format("%d", nanosecondsVal);
    StringBuilder builder = new StringBuilder();
    while (builder.length() < (NANO_SEC_STRING_LENGTH - nanoseconds.length())) {
      builder.append('0');
    }
    builder.append(nanoseconds);
    return builder.toString();
  }

  private static final Pattern pattern = Pattern.compile("%.");
  private static final Pattern CHARACTERS_WITH_NO_MOD_LITERAL_BEHIND_PATTERN
          = Pattern.compile("(?<!%)[a-zA-Z&&[^aydmshiHIMYDSEL]]+");
  private static final String MOD_LITERAL = "%";

  private DateTimeFormatterUtil() {
  }

  /**
   * Helper function to format a DATETIME according to a provided handler and matcher.
   * @param m A matcher generated using a format expression
   * @param handler Map of character patterns to their associated datetime format
   * @param datetime The datetime argument being formatted
   * @return A formatted string expression
   */
  static ExprValue getFormattedString(Matcher m,
                                      Map<String, DateTimeFormatHandler> handler,
                                      LocalDateTime datetime) {
    final StringBuffer cleanFormat = new StringBuffer();

    while (m.find()) {
      m.appendReplacement(cleanFormat,String.format("'%s'", m.group()));
    }
    m.appendTail(cleanFormat);

    final Matcher matcher = pattern.matcher(cleanFormat.toString());
    final StringBuffer format = new StringBuffer();
    while (matcher.find()) {
      matcher.appendReplacement(format,
          handler.getOrDefault(matcher.group(), (d) ->
                  String.format("'%s'", matcher.group().replaceFirst(MOD_LITERAL, "")))
              .getFormat(datetime));
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
    final Matcher m = CHARACTERS_WITH_NO_MOD_LITERAL_BEHIND_PATTERN
            .matcher(formatExpr.stringValue());

    return getFormattedString(m, DATE_HANDLERS, date);
  }

  /**
   * Format the date using the date format String.
   * @param timeExpr the date ExprValue of Date/Datetime/Timestamp/String type.
   * @param formatExpr the format ExprValue of String type.
   * @return Date formatted using format and returned as a String.
   */
  static ExprValue getFormattedTime(ExprValue timeExpr, ExprValue formatExpr) {
    final LocalDateTime time = LocalDateTime.of(LocalDate.now(), timeExpr.timeValue());
    final Matcher m = CHARACTERS_WITH_NO_MOD_LITERAL_BEHIND_PATTERN
        .matcher(formatExpr.stringValue());

    return getFormattedString(m, TIME_HANDLERS, time);
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
