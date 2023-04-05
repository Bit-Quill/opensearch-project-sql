/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import com.google.common.collect.ImmutableMap;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.sql.data.type.ExprType;

/**
 * Of type join with relations. See
 * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/join/">doc</a>
 */
@EqualsAndHashCode(callSuper = false)
public class OpenSearchDateType extends OpenSearchDataType {

  private static final OpenSearchDateType instance = new OpenSearchDateType();


  // a read-only collection of relations
  @Getter
  @Setter
  @EqualsAndHashCode.Exclude
  DateTimeFormatter formatter;

  @Getter
  @Setter
  @EqualsAndHashCode.Exclude
  String formatString;

  private OpenSearchDateType() {
    super(MappingType.Date);
    //TODO: Figure out how to apply the correct exprcoretype
    // (Maybe do whatever I initially did for timestampadd???
    this.formatter = null;
    this.formatString = "";
    //exprCoreType = UNKNOWN;
  }

  private OpenSearchDateType(DateTimeFormatter formatterArg, String formatStringArg) {
    super(MappingType.Date);
    //TODO: Figure out how to apply the correct exprcoretype
    // (Maybe do whatever I initially did for timestampadd???
    this.formatter = formatterArg;
    this.formatString = formatStringArg;
  }

  public static List<String> getFormatList(String formats) {
    if (formats == null || formats.isEmpty()) {
      return List.of();
    }
    return Arrays.stream(formats.split("\\|\\|")).map(String::trim).collect(Collectors.toList());
  }

//  public static List<DateFormatter> getNamedFormatters() {
//    return getFormatList(formatString).stream().filter(f -> {
//          try {
//            DateTimeFormatter.ofPattern(f);
//            return false;
//          } catch (Exception e) {
//            return true;
//          }
//        })
//        .map(DateFormatter::forPattern).collect(Collectors.toList());
//  }

  public List<DateFormatter> getNamedFormatters(String formats) {
    return getFormatList(formats).stream().filter(f -> {
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
    return getFormatList(formatString).stream().map(f -> {
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
  public static OpenSearchDateType create(String format) {

    //TODO: Filter out named formatters vs user defined here
    // Initialize the format based on the given string
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
    try {
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
      builder.append(DateTimeFormatter.ofPattern(format));
      try {
        builder.append(DateTimeFormatter.ofPattern(format));
      } catch (IllegalArgumentException e) {
        //do nothing
      }
    }
    } catch (IllegalArgumentException iae) {
      // invalid format - skipping
      // TODO: warn the user that the format is illegal in the mapping
    }

    var res = new OpenSearchDateType(builder.toFormatter(), format);
    return res;
  }

  public static OpenSearchDateType of(DateTimeFormatter format) {
    var res = new OpenSearchDateType();
    res.formatter = format;
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
    return OpenSearchDateType.create(this.formatString);
  }
}
