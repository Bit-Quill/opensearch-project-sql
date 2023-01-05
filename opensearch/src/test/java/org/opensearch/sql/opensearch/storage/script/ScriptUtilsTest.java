/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;

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
            OpenSearchDataType.of(OpenSearchDataType.Type.GeoPoint))),
        () -> assertEquals("field", ScriptUtils.convertTextToKeyword("field",
            OpenSearchDataType.of(OpenSearchDataType.Type.Keyword))),
        () -> assertEquals("field", ScriptUtils.convertTextToKeyword("field",
            OpenSearchDataType.of(OpenSearchDataType.Type.Integer)))
    );
  }

  @Test
  void non_text_types_with_nested_objects_arent_converted() {
    var objectType = OpenSearchDataType.of(OpenSearchDataType.Type.Object);
    objectType.getProperties().put("subfield", OpenSearchDataType.of(STRING));
    var arrayType = OpenSearchDataType.of(OpenSearchDataType.Type.Nested);
    objectType.getProperties().put("subfield", OpenSearchDataType.of(STRING));
    assertAll(
        () -> assertEquals("field", ScriptUtils.convertTextToKeyword("field", objectType)),
        () -> assertEquals("field", ScriptUtils.convertTextToKeyword("field", arrayType))
    );
  }

  @Test
  void text_type_without_fields_isnt_converted() {
    assertEquals("field", ScriptUtils.convertTextToKeyword("field",
        OpenSearchDataType.of(OpenSearchDataType.Type.Text)));
  }

  @Test
  void text_type_with_fields_is_converted() {
    var textWithKeywordType = OpenSearchDataType.of(OpenSearchDataType.Type.Text);
    textWithKeywordType.getFields().put("keyword",
        OpenSearchDataType.of(OpenSearchDataType.Type.Keyword));
    assertEquals("field.keyword", ScriptUtils.convertTextToKeyword("field", textWithKeywordType));
  }
}
