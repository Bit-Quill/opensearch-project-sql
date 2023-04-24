/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * Of type join with relations. See
 * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/join/">doc</a>
 */
@EqualsAndHashCode(callSuper = true)
public class OpenSearchDateType extends OpenSearchDataType {

  private static final OpenSearchDateType instance = new OpenSearchDateType();

  private static final String FORMAT_DELIMITER = "\\|\\|";


  // a read-only collection of relations

  @EqualsAndHashCode.Exclude
  String formatString;

  private OpenSearchDateType() {
    super(MappingType.Date);
    this.formatString = "";
  }

  public OpenSearchDateType(ExprCoreType type) {
    super(type);
    this.formatString = "";
  }

  private OpenSearchDateType(DateTimeFormatter formatterArg, String formatStringArg, MappingType mappingType) {
    super(mappingType);
    this.formatString = formatStringArg;
  }

  public List<String> getFormatList() {
    if (formatString == null) {
      return List.of();
    }
    return Arrays.stream(formatString.split(FORMAT_DELIMITER)).map(String::trim).collect(Collectors.toList());
  }

  public List<DateFormatter> getNamedFormatters() {
    return getFormatList().stream().filter(f -> {
          try {
            DateTimeFormatter.ofPattern(f);
            return false;
          } catch (Exception e) {
            return true;
          }
        })
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  public List<DateTimeFormatter> getRegularFormatters() {
    return getFormatList().stream().map(f -> {
          try {
            return DateTimeFormatter.ofPattern(f);
          } catch (Exception e) {
            return null;
          }
        })
        .filter(Objects::nonNull).collect(Collectors.toList());
  }

  /**
   * Create a Date type which has a LinkedHashMap defining all formats
   * @return A new type object.
   */
  public static OpenSearchDateType create(String format, MappingType mappingType) {
    //TODO: Filter out named formatters vs user defined here
    // Initialize the format based on the given string
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();

    if (format.contains("||")) {
      for (String token: format.split("\\|\\|")) {
        //try to append a pattern
        try {
          builder.appendPattern(token);
        } catch (IllegalArgumentException e) {
          //do nothing
        }
      }
    } else {
      try {
        builder.append(DateTimeFormatter.ofPattern(format));
      } catch (IllegalArgumentException e) {
        //do nothing
      }
    }
    var res = new OpenSearchDateType(builder.toFormatter(), format, mappingType);
    return res;
  }

  public static OpenSearchDateType of(DateTimeFormatter format) {
    var res = new OpenSearchDateType();
    return res;
  }

  public static OpenSearchDateType of() {
    return OpenSearchDateType.instance;
  }

  @Override
  public List<ExprType> getParent() {
    return List.of(STRING);
  }

  @Override
  public boolean shouldCast(ExprType other) {
    return false;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    if (this.mappingType == null) {
      return new OpenSearchDataType(this.exprCoreType);
    }
    return OpenSearchDateType.create(this.formatString, this.mappingType);
  }
}
