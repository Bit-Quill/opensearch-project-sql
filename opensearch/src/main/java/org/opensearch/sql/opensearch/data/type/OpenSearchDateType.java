/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.DATE;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
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

  private static final String FORMAT_DELIMITER = "\\|\\|";

  public static final List<FormatNames> SUPPORTED_NAMED_DATETIME_FORMATS = List.of(
    // TODO: add list of supported date/time formats
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
    // TODO add list of supported date formats
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

  // list of named formats that only support house/minute/second
  public static final List<FormatNames> SUPPORTED_NAMED_TIME_FORMATS = List.of(
      // TODO add list of supported time formats
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

  private OpenSearchDateType() {
    super(MappingType.Date);
    this.formatString = "";
  }

  private OpenSearchDateType(ExprCoreType exprCoreType) {
    super(MappingType.Date);
    this.formatString = "";
    this.exprCoreType = exprCoreType;
  }

  private OpenSearchDateType(String formatStringArg) {
    super(MappingType.Date);
    this.formatString = formatStringArg;
    this.exprCoreType = getExprTypeFromFormatString(formatStringArg);
  }

  /**
   * Retrieves and splits a user defined format string from the mapping into a list of formats.
   * @return A list of format names and user defined formats.
   */
  private List<String> getFormatList() {
    return Arrays.stream(formatString.split(FORMAT_DELIMITER))
        .map(String::trim)
        .collect(Collectors.toList());
  }


  /**
   * Retrieves a list of named formatters defined by OpenSearch.
   * @return a list of DateFormatters that can be used to parse a Date/Time/Timestamp.
   */
  public List<DateFormatter> getAllNamedFormatters() {
    if (formatString.isEmpty()) {
      return List.of();
    }

    return getFormatList().stream()
        .filter(formatString -> FormatNames.forName(formatString) != null)
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  public List<DateFormatter> getDateNamedFormatters() {
    return getFormatList().stream()
        .filter(formatString -> {
          FormatNames namedFormat = FormatNames.forName(formatString);
          return SUPPORTED_NAMED_DATE_FORMATS.contains(namedFormat);
        })
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  public List<DateFormatter> getTimeNamedFormatters() {
    return getFormatList().stream()
        .filter(formatString -> {
          FormatNames namedFormat = FormatNames.forName(formatString);
          return SUPPORTED_NAMED_TIME_FORMATS.contains(namedFormat);
        })
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  private ExprCoreType getExprTypeFromFormatString(String formatString) {
    if (formatString.isEmpty()) {
      // TODO: check the default formatter - and set it here instead of assuming that the default
      // is always a timestamp
      return ExprCoreType.TIMESTAMP;
    }

    List<DateFormatter> namedFormatters = getAllNamedFormatters();

    if (namedFormatters.isEmpty()) {
      // TODO: support custom format in <issue#>
      return ExprCoreType.TIMESTAMP;
    }

    // if there is nothing in the dateformatter that accepts a year/month/day, then
    // we can assume the type is strictly a Time object
    if (namedFormatters.size() == getTimeNamedFormatters().size()) {
      return ExprCoreType.TIME;
    }

    // if there is nothing in the dateformatter that accepts a hour/minute/second, then
    // we can assume the type is strictly a Date object
    if (namedFormatters.size() == getDateNamedFormatters().size()) {
      return DATE;
    }

    return ExprCoreType.TIMESTAMP;
  }

  /**
   * Create a Date type which has a LinkedHashMap defining all formats.
   * @return A new type object.
   */
  public static OpenSearchDateType create(String format) {
    return new OpenSearchDateType(format);
  }

  public static OpenSearchDateType of(ExprCoreType exprCoreType) {
    return new OpenSearchDateType(exprCoreType);
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
    return OpenSearchDateType.create(this.formatString);
  }
}
