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
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.joda.time.DateTime;
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
  @EqualsAndHashCode.Exclude
  DateTimeFormatter formatter;

  @Getter
  @EqualsAndHashCode.Exclude
  String formatString;

  private OpenSearchDateType() {
    super(MappingType.Date);
    //TODO: Figure out how to apply the correct exprcoretype
    // (Maybe do whatever I initially did for timestampadd???
    //exprCoreType = UNKNOWN;
  }

  /**
   * Create a Date type which has a LinkedHashMap defining all formats
   * @return A new type object.
   */
  public static OpenSearchDateType create(String format) {
    var res = new OpenSearchDateType();

    //TODO: Temp. Refactor this to exist in DateTimeFormatters.java
    DateTimeFormatter predefinedPattern = DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss");
    final Map<String, DateTimeFormatter> NAMED_FORMATTERS = ImmutableMap.<String, DateTimeFormatter>builder()
        .put("date_optional_time", predefinedPattern)
        .put("epoch_millis", predefinedPattern)
        .put("epoch_second", predefinedPattern)
        .build();

    //TODO: Filter out named formatters vs user defined here
    // Initialize the format based on the given string
    try {
      if (format.contains("||")) {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        for (String token: format.split("\\|\\|")) {
          //Use either a predefined formatter, or a user defined one.
          if (NAMED_FORMATTERS.containsKey(token)){
            builder.append(NAMED_FORMATTERS.get(token));
          } else {
            builder.appendPattern(token);
          }
        }
        res.formatter = builder.toFormatter();
      } else {
        if (NAMED_FORMATTERS.containsKey(format)){
          res.formatter= NAMED_FORMATTERS.get(format);
        } else {
          res.formatter = DateTimeFormatter.ofPattern(format);
        }

      }
    } catch (IllegalArgumentException iae) {
      // invalid format - skipping
      // TODO: warn the user that the format is illegal in the mapping
    }
    res.formatString = format;
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
