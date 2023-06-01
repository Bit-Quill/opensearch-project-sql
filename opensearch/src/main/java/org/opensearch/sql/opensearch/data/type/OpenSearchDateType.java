/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.common.time.DateFormatter.splitCombinedPatterns;
import static org.opensearch.common.time.DateFormatter.strip8Prefix;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.FormatNames;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * Date type with support for predefined and custom formats read from the index mapping.
 */
@EqualsAndHashCode(callSuper = true)
public class OpenSearchDateType extends OpenSearchDataType {

  private static final OpenSearchDateType instance = new OpenSearchDateType();

  public static final List<FormatNames> SUPPORTED_NAMED_DATETIME_FORMATS = List.of(
      FormatNames.ISO8601,
      FormatNames.EPOCH_MILLIS,
      FormatNames.EPOCH_SECOND,
      FormatNames.BASIC_DATE_TIME,
      FormatNames.BASIC_DATE_TIME_NO_MILLIS,
      FormatNames.BASIC_ORDINAL_DATE_TIME,
      FormatNames.BASIC_ORDINAL_DATE_TIME_NO_MILLIS,
      FormatNames.BASIC_WEEK_DATE_TIME,
      FormatNames.STRICT_BASIC_WEEK_DATE_TIME,
      FormatNames.BASIC_WEEK_DATE_TIME_NO_MILLIS,
      FormatNames.STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS,
      FormatNames.BASIC_WEEK_DATE,
      FormatNames.STRICT_BASIC_WEEK_DATE,
      FormatNames.DATE_OPTIONAL_TIME,
      FormatNames.STRICT_DATE_OPTIONAL_TIME,
      FormatNames.STRICT_DATE_OPTIONAL_TIME_NANOS,
      FormatNames.DATE_TIME,
      FormatNames.STRICT_DATE_TIME,
      FormatNames.DATE_TIME_NO_MILLIS,
      FormatNames.STRICT_DATE_TIME_NO_MILLIS,
      FormatNames.DATE_HOUR_MINUTE_SECOND_FRACTION,
      FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION,
      FormatNames.DATE_HOUR_MINUTE_SECOND_FRACTION,
      FormatNames.DATE_HOUR_MINUTE_SECOND_MILLIS,
      FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS,
      FormatNames.DATE_HOUR_MINUTE_SECOND,
      FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND,
      FormatNames.DATE_HOUR_MINUTE,
      FormatNames.STRICT_DATE_HOUR_MINUTE,
      FormatNames.DATE_HOUR,
      FormatNames.STRICT_DATE_HOUR,
      FormatNames.ORDINAL_DATE_TIME,
      FormatNames.STRICT_ORDINAL_DATE_TIME,
      FormatNames.ORDINAL_DATE_TIME_NO_MILLIS,
      FormatNames.STRICT_ORDINAL_DATE_TIME_NO_MILLIS,
      FormatNames.WEEK_DATE_TIME,
      FormatNames.STRICT_WEEK_DATE_TIME,
      FormatNames.WEEK_DATE_TIME_NO_MILLIS,
      FormatNames.STRICT_WEEK_DATE_TIME_NO_MILLIS
  );

  // list of named formats that only support year/month/day
  public static final List<FormatNames> SUPPORTED_NAMED_DATE_FORMATS = List.of(
      FormatNames.BASIC_DATE,
      FormatNames.BASIC_ORDINAL_DATE,
      FormatNames.DATE,
      FormatNames.STRICT_DATE,
      FormatNames.YEAR_MONTH_DAY,
      FormatNames.STRICT_YEAR_MONTH_DAY,
      FormatNames.YEAR_MONTH,
      FormatNames.STRICT_YEAR_MONTH,
      FormatNames.YEAR,
      FormatNames.STRICT_YEAR,
      FormatNames.ORDINAL_DATE,
      FormatNames.STRICT_ORDINAL_DATE,
      FormatNames.WEEK_DATE,
      FormatNames.STRICT_WEEK_DATE,
      FormatNames.WEEKYEAR_WEEK_DAY,
      FormatNames.STRICT_WEEKYEAR_WEEK_DAY,
      FormatNames.WEEK_YEAR,
      FormatNames.WEEK_YEAR_WEEK,
      FormatNames.STRICT_WEEKYEAR_WEEK,
      FormatNames.WEEKYEAR,
      FormatNames.STRICT_WEEKYEAR
  );

  // list of named formats that only support hour/minute/second
  public static final List<FormatNames> SUPPORTED_NAMED_TIME_FORMATS = List.of(
      FormatNames.BASIC_TIME,
      FormatNames.BASIC_TIME_NO_MILLIS,
      FormatNames.BASIC_T_TIME,
      FormatNames.BASIC_T_TIME_NO_MILLIS,
      FormatNames.TIME,
      FormatNames.STRICT_TIME,
      FormatNames.TIME_NO_MILLIS,
      FormatNames.STRICT_TIME_NO_MILLIS,
      FormatNames.HOUR_MINUTE_SECOND_FRACTION,
      FormatNames.STRICT_HOUR_MINUTE_SECOND_FRACTION,
      FormatNames.HOUR_MINUTE_SECOND_MILLIS,
      FormatNames.STRICT_HOUR_MINUTE_SECOND_MILLIS,
      FormatNames.HOUR_MINUTE_SECOND,
      FormatNames.STRICT_HOUR_MINUTE_SECOND,
      FormatNames.HOUR_MINUTE,
      FormatNames.STRICT_HOUR_MINUTE,
      FormatNames.HOUR,
      FormatNames.STRICT_HOUR,
      FormatNames.T_TIME,
      FormatNames.STRICT_T_TIME,
      FormatNames.T_TIME_NO_MILLIS,
      FormatNames.STRICT_T_TIME_NO_MILLIS
  );

  @EqualsAndHashCode.Exclude
  String formatString;
  @Getter
  List<DateFormatter> dateFormatters;
  @Getter
  List<DateFormatter> timeFormatters;
  @Getter
  List<DateFormatter> dateTimeFormatters;
  @Getter
  List<DateFormatter> customFormatters;

  private OpenSearchDateType() {
    super(MappingType.Date);
    this.formatString = "";
    this.dateFormatters = getDateNamedFormatters("");
    this.timeFormatters = getTimeNamedFormatters("");
    this.dateTimeFormatters = getDateTimeNamedFormatters("");
    this.customFormatters = getAllCustomFormatters("");
  }

  private OpenSearchDateType(ExprCoreType exprCoreType) {
    this();
    this.exprCoreType = exprCoreType;
  }

  private OpenSearchDateType(ExprType exprType) {
    this();
    this.exprCoreType = (ExprCoreType) exprType;
  }

  private OpenSearchDateType(String formatStringArg) {
    super(MappingType.Date);
    this.formatString = formatStringArg;
    this.dateFormatters = getDateNamedFormatters(formatStringArg);
    this.timeFormatters = getTimeNamedFormatters(formatStringArg);
    this.dateTimeFormatters = getDateTimeNamedFormatters(formatStringArg);
    this.customFormatters = getAllCustomFormatters(formatStringArg);
    this.exprCoreType = getExprTypeFromFormatString(formatStringArg);
  }

  /**
   * Retrieves and splits a user defined format string from the mapping into a list of formats.
   * @return A list of format names and user defined formats.
   */
  private List<String> getFormatList(String formatString) {
    String format = strip8Prefix(formatString);
    List<String> patterns = splitCombinedPatterns(format);
    return patterns;
  }


  /**
   * Retrieves a list of named OpenSearch formatters given by user mapping.
   * @return a list of DateFormatters that can be used to parse a Date/Time/Timestamp.
   */
  public List<DateFormatter> getAllNamedFormatters(String formatString) {
    return getFormatList(formatString).stream()
        .filter(formats -> FormatNames.forName(formats) != null)
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  /**
   * Retrieves a list of custom formatters defined by the user.
   * @return a list of DateFormatters that can be used to parse a Date/Time/Timestamp.
   */
  public List<DateFormatter> getAllCustomFormatters(String formatString) {
    return getFormatList(formatString).stream()
        .filter(formats -> FormatNames.forName(formats) == null)
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  /**
   * Retrieves a list of named formatters that format for dates.
   *
   * @return a list of DateFormatters that can be used to parse a Date.
   */
  public List<DateFormatter> getDateTimeNamedFormatters(String formatString) {
    return getFormatList(formatString).stream()
        .filter(formats -> {
          FormatNames namedFormat = FormatNames.forName(formats);
          return namedFormat == null ? false : SUPPORTED_NAMED_DATE_FORMATS.contains(namedFormat);
        })
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  /**
   * Retrieves a list of named formatters that format for dates.
   *
   * @return a list of DateFormatters that can be used to parse a Date.
   */
  public List<DateFormatter> getDateNamedFormatters(String formatString) {
    return getFormatList(formatString).stream()
        .filter(formats -> {
          FormatNames namedFormat = FormatNames.forName(formats);
          return namedFormat == null ? false : SUPPORTED_NAMED_DATE_FORMATS.contains(namedFormat);
        })
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  /**
   * Retrieves a list of named formatters that format for Times.
   *
   * @return a list of DateFormatters that can be used to parse a Time.
   */
  public List<DateFormatter> getTimeNamedFormatters(String formatString) {
    return getFormatList(formatString).stream()
        .filter(formats -> {
          FormatNames namedFormat = FormatNames.forName(formats);
          return namedFormat == null ? false : SUPPORTED_NAMED_TIME_FORMATS.contains(namedFormat);
        })
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  private ExprCoreType getExprTypeFromCustomFormatString(List<DateFormatter> formatters) {
    Pattern timeFormatChars = Pattern.compile("[HmsSZ]");
    Pattern dateFormatChars = Pattern.compile("[yYMwdDe]");
    boolean isTime = false;
    boolean isDate = false;

    for (DateFormatter formatter: formatters) {
      if (timeFormatChars.matcher(formatter.pattern()).find()) {
        isTime = true;
      }
      if (dateFormatChars.matcher(formatter.pattern()).find()) {
        isDate = true;
      }
    }
    if (isTime && !isDate) {
      return TIME;
    }
    if (isDate && !isTime) {
      return DATE;
    }

    // Default type if pattern doesn't match characters or if is time and is date
    return TIMESTAMP;
  }

  private ExprCoreType getExprTypeFromFormatString(String formatString) {
    if (formatString.isEmpty()) {
      // FOLLOW-UP: check the default formatter - and set it here instead
      // of assuming that the default is always a timestamp
      return TIMESTAMP;
    }

    if (!customFormatters.isEmpty()) {
      // TODO: Check for namedFormatters as well
      return getExprTypeFromCustomFormatString(customFormatters);
    }

    // if there is nothing in the dateformatter that accepts a year/month/day, then
    // we can assume the type is strictly a Time object
    if (!timeFormatters.isEmpty() && customFormatters.isEmpty() && dateTimeFormatters.isEmpty() && dateTimeFormatters.isEmpty()) {
      return TIME;
    }

    // if there is nothing in the dateformatter that accepts a hour/minute/second, then
    // we can assume the type is strictly a Date object
    if (!dateFormatters.isEmpty() && timeFormatters.isEmpty() && customFormatters.isEmpty() && dateTimeFormatters.isEmpty()) {
      return DATE;
    }

    // According to the user mapping, this field may contain a DATE or a TIME
    return TIMESTAMP;
  }

  /**
   * Check if ExprType is compatible for creation of OpenSearchDateType object.
   *
   * @param exprType type of the field in the SQL query
   * @return a boolean if type is a date/time/timestamp type
   */
  public static boolean isDateTypeCompatible(ExprType exprType) {
    if (!(exprType instanceof ExprCoreType)) {
      return false;
    }
    switch ((ExprCoreType) exprType) {
      case TIMESTAMP:
      case DATETIME:
      case DATE:
      case TIME:
        return true;
      default:
        return false;
    }
  }

  /**
   * Create a Date type which has a LinkedHashMap defining all formats.
   * @return A new type object.
   */
  public static OpenSearchDateType of(String format) {
    return new OpenSearchDateType(format);
  }

  public static OpenSearchDateType of(ExprCoreType exprCoreType) {
    return new OpenSearchDateType(exprCoreType);
  }

  public static OpenSearchDateType of(ExprType exprType) {
    return new OpenSearchDateType(exprType);
  }

  public static OpenSearchDateType of() {
    return OpenSearchDateType.instance;
  }

  @Override
  public List<ExprType> getParent() {
    return List.of(this.exprCoreType);
  }

  @Override
  public boolean shouldCast(ExprType other) {
    return false;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    if (this.formatString.isEmpty()) {
      return OpenSearchDateType.of(this.exprCoreType);
    }
    return OpenSearchDateType.of(this.formatString);
  }
}
