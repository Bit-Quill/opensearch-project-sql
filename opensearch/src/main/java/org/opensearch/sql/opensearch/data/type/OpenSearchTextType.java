/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.opensearch.sql.data.type.ExprType;

/**
 * The type of a text value. See
 * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/text/">doc</a>
 */
public class OpenSearchTextType extends OpenSearchDataType {

  private static final OpenSearchTextType instance = new OpenSearchTextType();

  // text could have fields
  // a read-only collection
  @Getter
  Map<String, OpenSearchDataType> fields = ImmutableMap.of();

  private OpenSearchTextType() {
    super(MappingType.Text);
    exprCoreType = STRING;
  }

  /**
   * Constructs a Text Type using the passed in fields argument.
   * @param fields The fields to be used to construct the text type.
   * @return A new OpenSearchTextType object
   */
  public static OpenSearchTextType of(Map<String, OpenSearchDataType> fields) {
    var res = new OpenSearchTextType();
    res.fields = fields;
    return res;
  }

  public static OpenSearchTextType of() {
    return OpenSearchTextType.instance;
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
    return OpenSearchTextType.of(Map.copyOf(this.fields));
  }

  @Override
  public String convertFieldForSearchQuery(String fieldName) {
    if (fields.size() > 1) {
      // TODO or pick first?
      throw new RuntimeException("too many text fields");
    }
    if (fields.size() == 0) {
      return fieldName;
    }
    // TODO what if field is not a keyword
    // https://github.com/opensearch-project/sql/issues/1112
    return String.format("%s.%s", fieldName, fields.keySet().toArray()[0]);
  }
}
