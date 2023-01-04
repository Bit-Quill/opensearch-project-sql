/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.mapping;

import static java.util.Collections.emptyMap;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.EnumUtils;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;

/**
 * OpenSearch index mapping. Because there is no specific behavior for different field types,
 * string is used to represent field types.
 */
@ToString
public class IndexMapping {

  /** Field mappings from field name to field type in OpenSearch date type system. */
  @Getter
  private final Map<String, OpenSearchDataType> fieldMappings;

  // TODO remove, used in tests only
  public IndexMapping(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings.entrySet().stream()
        .filter(e -> EnumUtils.isValidEnumIgnoreCase(OpenSearchDataType.Type.class, e.getValue()))
        .collect(Collectors.toMap(e -> e.getKey(), e -> OpenSearchDataType.of(
            EnumUtils.getEnumIgnoreCase(OpenSearchDataType.Type.class, e.getValue()))
        ));
  }

  public IndexMapping(MappingMetadata metaData) {
    this.fieldMappings = parseMapping(metaData.getSourceAsMap());
  }

  /**
   * How many fields in the index (after flatten).
   *
   * @return field size
   */
  public int size() {
    return fieldMappings.size();
  }

  /**
   * Return field type by its name.
   *
   * @param fieldName field name
   * @return field type in string. Or null if not exist.
   */
  public String getFieldType(String fieldName) {
    if (!fieldMappings.containsKey(fieldName)) {
      return null;
    }
    return fieldMappings.get(fieldName).typeName();
  }

  @SuppressWarnings("unchecked")
  private Map<String, OpenSearchDataType> parseMapping(Map<String, Object> indexMapping) {
    Map<String, OpenSearchDataType> result = new HashMap<>();
    Map<String, Object> mappingInfo = null;
    if (indexMapping.containsKey("properties")) {
      mappingInfo = (Map<String, Object>)indexMapping.get("properties");
    } else if (indexMapping.containsKey("fields")) {
      mappingInfo = (Map<String, Object>)indexMapping.get("fields");
    }
    if (mappingInfo != null) {
      mappingInfo.forEach((k, v) -> {
          var innerMap = (Map<String, Object>)v;
          // TODO: confirm that only `object` mappings can omit `type` field.
          var type = ((String) innerMap.getOrDefault("type", "object")).replace("_", "");
          if (!EnumUtils.isValidEnumIgnoreCase(OpenSearchDataType.Type.class, type)) {
            // unknown type, e.g. `alias`
            // TODO resolve alias reference
            return;
          }
          var value = OpenSearchDataType.of(EnumUtils.getEnumIgnoreCase(OpenSearchDataType.Type.class, type));
          // TODO read formats for date type
          if (innerMap.containsKey("properties")) {
            value.getProperties().putAll(parseMapping(innerMap));
          } else if (innerMap.containsKey("fields")) {
            value.getFields().putAll(parseMapping(innerMap));
          }
          result.put(k, value);
        });
    }
    return result;
  }
}
