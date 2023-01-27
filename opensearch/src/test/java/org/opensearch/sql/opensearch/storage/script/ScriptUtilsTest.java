/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class ScriptUtilsTest {

  @Test
  void non_text_types_arent_converted() {
    assertAll(
        () -> assertEquals("field", ScriptUtils.convertTextToKeyword("field",
            OpenSearchDataType.of(INTEGER))),
        () -> assertEquals("field", ScriptUtils.convertTextToKeyword("field",
            OpenSearchDataType.of(STRING))),
        () -> assertEquals("field", ScriptUtils.convertTextToKeyword("field",
            OpenSearchDataType.of(OpenSearchDataType.MappingType.GeoPoint))),
        () -> assertEquals("field", ScriptUtils.convertTextToKeyword("field",
            OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword))),
        () -> assertEquals("field", ScriptUtils.convertTextToKeyword("field",
            OpenSearchDataType.of(OpenSearchDataType.MappingType.Integer)))
    );
  }

  @Test
  void non_text_types_with_nested_objects_arent_converted() {
    var objectType = OpenSearchDataType.of(OpenSearchDataType.MappingType.Object,
        Map.of("subfield", OpenSearchDataType.of(STRING)), Map.of());
    var arrayType = OpenSearchDataType.of(OpenSearchDataType.MappingType.Nested,
        Map.of("subfield", OpenSearchDataType.of(STRING)), Map.of());
    assertAll(
        () -> assertEquals("field", ScriptUtils.convertTextToKeyword("field", objectType)),
        () -> assertEquals("field", ScriptUtils.convertTextToKeyword("field", arrayType))
    );
  }

  @Test
  void text_type_without_fields_isnt_converted() {
    assertEquals("field", ScriptUtils.convertTextToKeyword("field",
        OpenSearchDataType.of(OpenSearchDataType.MappingType.Text)));
  }

  @Test
  void text_type_with_fields_is_converted() {
    var textWithKeywordType = new OpenSearchTextType(Map.of("keyword",
        OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)));
    assertEquals("field.keyword", ScriptUtils.convertTextToKeyword("field", textWithKeywordType));
  }
}
