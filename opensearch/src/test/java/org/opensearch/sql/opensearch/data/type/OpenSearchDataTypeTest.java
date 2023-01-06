/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.Type.Keyword;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.Type.Text;

import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

class OpenSearchDataTypeTest {

  private static OpenSearchDataType textType;
  private static OpenSearchDataType textKeywordType;

  @BeforeAll
  private static void setUpTypes() {
    textType = OpenSearchDataType.of(Text);
    textKeywordType = OpenSearchDataType.of(Text);
    textKeywordType.getFields().put("words", OpenSearchTextType.of(Keyword));
  }

  @Test
  public void testIsCompatible() {
    assertTrue(STRING.isCompatible(textType));
    assertFalse(textType.isCompatible(STRING));

    assertTrue(STRING.isCompatible(textKeywordType));
    assertTrue(textType.isCompatible(textKeywordType));
  }

  @Test
  public void testTypeName() {
    assertEquals("text", textType.typeName());
    assertEquals("text", textKeywordType.typeName());
  }

  @Test
  public void legacyTypeName() {
    assertEquals("text", textType.legacyTypeName());
    assertEquals("text", textKeywordType.legacyTypeName());
  }

  @Test
  public void testShouldCast() {
    assertFalse(textType.shouldCast(STRING));
    assertFalse(textKeywordType.shouldCast(STRING));
  }

  private static Stream<Arguments> getTestDataWithType() {
    return Stream.of(
        Arguments.of(Text, "text", new OpenSearchTextType()),
        Arguments.of(Keyword, "keyword", STRING),
        Arguments.of(OpenSearchDataType.Type.Byte, "byte", BYTE),
        Arguments.of(OpenSearchDataType.Type.Short, "short", SHORT),
        Arguments.of(OpenSearchDataType.Type.Integer, "integer", INTEGER),
        Arguments.of(OpenSearchDataType.Type.Long, "long", LONG),
        Arguments.of(OpenSearchDataType.Type.HalfFloat, "half_float", FLOAT),
        Arguments.of(OpenSearchDataType.Type.Float, "float", FLOAT),
        Arguments.of(OpenSearchDataType.Type.ScaledFloat, "scaled_float", DOUBLE),
        Arguments.of(OpenSearchDataType.Type.Double, "double", DOUBLE),
        Arguments.of(OpenSearchDataType.Type.Boolean, "boolean", BOOLEAN),
        Arguments.of(OpenSearchDataType.Type.Date, "date", TIMESTAMP),
        Arguments.of(OpenSearchDataType.Type.Object, "object", STRUCT),
        Arguments.of(OpenSearchDataType.Type.Nested, "nested", ARRAY),
        Arguments.of(OpenSearchDataType.Type.GeoPoint, "geo_point", new OpenSearchGeoPointType()),
        Arguments.of(OpenSearchDataType.Type.Binary, "binary", new OpenSearchBinaryType()),
        Arguments.of(OpenSearchDataType.Type.Ip, "ip", new OpenSearchIpType())
    );
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("getTestDataWithType")
  public void ofType(OpenSearchDataType.Type mappingType, String name, ExprType dataType) {
    var type = OpenSearchDataType.of(mappingType);
    assertAll(
        () -> assertEquals(name, type.typeName()),
        () -> assertEquals(name, type.legacyTypeName()),
        () -> assertEquals(dataType, type.getExprType())
    );
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(ExprCoreType.class)
  public void ofExprCoreType(ExprCoreType coreType) {
    assumeFalse(coreType == UNKNOWN);
    var type = OpenSearchDataType.of(coreType);
    assertAll(
        () -> assertEquals(coreType.toString(), type.typeName()),
        () -> assertEquals(coreType.toString(), type.legacyTypeName()),
        () -> assertEquals(coreType, type.getExprType())
    );
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(ExprCoreType.class)
  public void ofOpenSearchDataTypeFromExprCoreType(ExprCoreType coreType) {
    var type = OpenSearchDataType.of(coreType);
    var derivedType = OpenSearchDataType.of(type);
    assertEquals(type, derivedType);
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(OpenSearchDataType.Type.class)
  public void ofOpenSearchDataTypeFromMappingType(OpenSearchDataType.Type mappingType) {
    var type = OpenSearchDataType.of(mappingType);
    var derivedType = OpenSearchDataType.of(type);
    assertEquals(type, derivedType);
  }

  @Test
  // cloneEmpty doesn't clone properties, but clones fields and other attributes
  public void cloneEmpty() {
    var type = new TestType(OpenSearchDataType.Type.Object);
    type.getProperties().put("val", OpenSearchDataType.of(INTEGER));
    type.getFields().put("words", OpenSearchDataType.of(STRING));
    var clone = type.cloneEmpty();
    assertAll(
        // can compare because `properties` and `fields` are marked as @EqualsAndHashCode.Exclude
        () -> assertEquals(type, clone),
        () -> assertEquals(type.getFields(), clone.getFields()),
        () -> assertTrue(clone.getProperties().isEmpty())
    );
  }

  // Following structure of nested objects should be flattened
  // =====================
  // type
  //    |- subtype
  //    |        |
  //    |        |- subsubtype
  //    |        |           |
  //    |        |           |- textWithKeywordType
  //    |        |           |                    |- keyword
  //    |        |           |
  //    |        |           |- INTEGER
  //    |        |
  //    |        |- GeoPoint
  //    |        |- textWithFieldsType
  //    |                            |- words
  //    |
  //    |- text
  //    |- keyword
  // =================
  // as
  // =================
  // type : Object
  // type.subtype : Object
  // type.subtype.subsubtype : Object
  // type.subtype.subsubtype.textWithKeywordType : Text
  //                                           |- keyword : Keyword
  // type.subtype.subsubtype.INTEGER : INTEGER
  // type.subtype.geo_point : GeoPoint
  // type.subtype.textWithFieldsType: Text
  //                               |- words : Keyword
  // type.text : Text
  // type.keyword : Keyword
  // ==================
  // Objects are flattened, but Texts aren't
  // TODO Arrays
  @Test
  public void traverseAndFlatten() {
    var type = OpenSearchDataType.of(OpenSearchDataType.Type.Object);
    var subtype = OpenSearchDataType.of(OpenSearchDataType.Type.Object);
    var subsubtype = OpenSearchDataType.of(OpenSearchDataType.Type.Object);
    var textWithKeywordType = OpenSearchDataType.of(OpenSearchDataType.Type.Text);
    textWithKeywordType.getFields().put("keyword",
        OpenSearchDataType.of(OpenSearchDataType.Type.Keyword));
    subsubtype.getProperties().put("textWithKeywordType", textWithKeywordType);
    subsubtype.getProperties().put("INTEGER", OpenSearchDataType.of(INTEGER));
    subtype.getProperties().put("subsubtype", subsubtype);
    var textWithFieldsType = OpenSearchDataType.of(OpenSearchDataType.Type.Text);
    textWithFieldsType.getFields().put("words",
        OpenSearchDataType.of(OpenSearchDataType.Type.Keyword));
    subtype.getProperties().put("textWithFieldsType", textWithFieldsType);
    subtype.getProperties().put("geo_point", new OpenSearchGeoPointType());
    type.getProperties().put("subtype", subtype);
    type.getProperties().put("keyword", OpenSearchDataType.of(OpenSearchDataType.Type.Keyword));
    type.getProperties().put("text", OpenSearchDataType.of(OpenSearchDataType.Type.Text));

    var flattened = OpenSearchDataType.traverseAndFlatten(Map.of("type", type));
    assertAll(
        () -> assertEquals(9, flattened.size()),
        () -> assertTrue(flattened.get("type").getProperties().isEmpty()),
        () -> assertTrue(flattened.get("type.subtype").getProperties().isEmpty()),
        () -> assertTrue(flattened.get("type.subtype.subsubtype").getProperties().isEmpty()),

        () -> assertEquals(type, flattened.get("type")),
        () -> assertEquals(subtype, flattened.get("type.subtype")),
        () -> assertEquals(subsubtype, flattened.get("type.subtype.subsubtype")),

        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.Type.Keyword),
            flattened.get("type.keyword")),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.Type.Text),
            flattened.get("type.text")),

        () -> assertEquals(new OpenSearchGeoPointType(), flattened.get("type.subtype.geo_point")),
        () -> assertEquals(textWithFieldsType, flattened.get("type.subtype.textWithFieldsType")),

        () -> assertEquals(textWithKeywordType,
            flattened.get("type.subtype.subsubtype.textWithKeywordType")),
        () -> assertEquals(OpenSearchDataType.of(INTEGER),
            flattened.get("type.subtype.subsubtype.INTEGER"))
    );
  }

  private static class TestType extends OpenSearchDataType {
    public TestType(OpenSearchDataType.Type type) {
      this.exprCoreType = OpenSearchDataType.of(type).exprCoreType;
      this.type = type;
    }

    public OpenSearchDataType cloneEmpty() {
      return super.cloneEmpty();
    }
  }
}
