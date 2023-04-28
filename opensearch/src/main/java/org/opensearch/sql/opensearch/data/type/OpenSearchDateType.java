/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.sql.data.type.ExprType;

/**
 * Date type with support for predefined and custom formats read from the index mapping.
 */
@EqualsAndHashCode(callSuper = true)
public class OpenSearchDateType extends OpenSearchDataType {

  private static final OpenSearchDateType instance = new OpenSearchDateType();

  private static final String FORMAT_DELIMITER = "\\|\\|";

  @EqualsAndHashCode.Exclude
  String formatString;

  private OpenSearchDateType() {
    super(MappingType.Date);
    this.formatString = "";
  }

  private OpenSearchDateType(String formatStringArg) {
    super(MappingType.Date);
    this.formatString = formatStringArg;
  }

  /**
   * Retrieves and splits a user defined format string from the mapping into a list of formats.
   * @return A list of format names and user defined formats.
   */
  public List<String> getFormatList() {
    return Arrays.stream(formatString.split(FORMAT_DELIMITER))
        .map(String::trim)
        .collect(Collectors.toList());
  }


  /**
   * Retrieves named formatters defined by OpenSearch.
   * @return a list of DateFormatters that can be used to parse a Date/Time/Timestamp.
   */
  public List<DateFormatter> getNamedFormatters() {
    return getFormatList().stream().filter(f -> {
      try {
        DateTimeFormatter.ofPattern(f);
        //TODO: Filter off of a constant list of formats
        return false;
      } catch (IllegalArgumentException e) {
        return true;
      }
    }).map(DateFormatter::forPattern).collect(Collectors.toList());
  }


  /**
   * Creates DateTimeFormatters based on a custom user format defined in the index mapping.
   * @return a list of DateTimeFormatters that can be used to parse a Date/Time/Timestamp.
   */
  public List<DateTimeFormatter> getRegularFormatters() {
    return getFormatList().stream().map(f -> {
      try {
        return DateTimeFormatter.ofPattern(f);
      } catch (IllegalArgumentException e) {
        return null;
      }
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  /**
   * Create a Date type which has a LinkedHashMap defining all formats.
   * @return A new type object.
   */
  public static OpenSearchDateType create(String format) {
    return new OpenSearchDateType(format);
  }

  public static OpenSearchDateType of() {
    return OpenSearchDateType.instance;
  }

  @Override
  public boolean shouldCast(ExprType other) {
    return false;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    return OpenSearchDateType.create(this.formatString);
  }
}
