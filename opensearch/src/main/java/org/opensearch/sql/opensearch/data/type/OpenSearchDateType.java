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
  @EqualsAndHashCode.Exclude
  DateTimeFormatter format;

  private OpenSearchDateType() {
    super(MappingType.Date);
    exprCoreType = UNKNOWN;
  }

  /**
   * Create a Date type which has a LinkedHashMap defining all formats
   * @return A new type object.
   */
  public static OpenSearchDateType of(String format) {
    var res = new OpenSearchDateType();

    // Initialize the format based on the given string
    try {
      if (format.contains("||")) {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        for (String token: format.split("\\|\\|")) {
          builder.appendPattern(token);
        }
        res.format = builder.toFormatter();
      } else {
        res.format = DateTimeFormatter.ofPattern(format);
      }
    } catch (IllegalArgumentException iae) {
      // invalid format - skipping
      // TODO: warn the user that the format is illegal in the mapping
    }
    return res;
  }

  public static OpenSearchDateType of(DateTimeFormatter format) {
    var res = new OpenSearchDateType();
    res.format = format;
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
    return OpenSearchDateType.of(this.format);
  }
}
