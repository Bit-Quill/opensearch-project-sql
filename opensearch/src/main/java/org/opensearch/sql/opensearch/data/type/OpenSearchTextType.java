/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.data.type.ExprType;

/**
 * The type of a text value. See
 * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/text/">doc</a>
 */
@EqualsAndHashCode(callSuper = false)
public class OpenSearchTextType extends OpenSearchDataType {

  public OpenSearchTextType() {
    super(MappingType.Text);
    exprCoreType = UNKNOWN;
  }

  public OpenSearchTextType(Map<String, OpenSearchDataType> fields) {
    this();
    this.fields = ImmutableMap.copyOf(fields);
  }

  @Override
  public List<ExprType> getParent() {
    return List.of(STRING);
  }

  @Override
  public boolean shouldCast(ExprType other) {
    return false;
  }

  public Map<String, OpenSearchDataType> getFields() {
    return fields;
  }

  /**
   * Text field doesn't have doc value (exception thrown even when you call "get")
   * Limitation: assume inner field name is always "keyword".
   */
  public static String convertTextToKeyword(String fieldName, ExprType fieldType) {
    if (fieldType instanceof OpenSearchTextType
        && ((OpenSearchTextType) fieldType).getFields().size() > 0) {
      return fieldName + ".keyword";
    }
    return fieldName;
  }
}
