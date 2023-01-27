/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType.Invalid;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType.Keyword;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType.Text;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.reflect.FieldUtils;
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
    textKeywordType = new OpenSearchTextType(Map.of("words", OpenSearchTextType.of(Keyword)));
  }

  @Test
  public void testIsCompatible() {
    assertTrue(STRING.isCompatible(textType));
    assertFalse(textType.isCompatible(STRING));

    assertTrue(STRING.isCompatible(textKeywordType));
    assertTrue(textType.isCompatible(textKeywordType));
  }

  // `typeName` and `legacyTypeName` return different things:
  // https://github.com/opensearch-project/sql/issues/1296
  @Test
  public void testTypeName() {
    assertEquals("string", textType.typeName());
    assertEquals("string", textKeywordType.typeName());
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
        Arguments.of(OpenSearchDataType.MappingType.Byte, "byte", BYTE),
        Arguments.of(OpenSearchDataType.MappingType.Short, "short", SHORT),
        Arguments.of(OpenSearchDataType.MappingType.Integer, "integer", INTEGER),
        Arguments.of(OpenSearchDataType.MappingType.Long, "long", LONG),
        Arguments.of(OpenSearchDataType.MappingType.HalfFloat, "half_float", FLOAT),
        Arguments.of(OpenSearchDataType.MappingType.Float, "float", FLOAT),
        Arguments.of(OpenSearchDataType.MappingType.ScaledFloat, "scaled_float", DOUBLE),
        Arguments.of(OpenSearchDataType.MappingType.Double, "double", DOUBLE),
        Arguments.of(OpenSearchDataType.MappingType.Boolean, "boolean", BOOLEAN),
        Arguments.of(OpenSearchDataType.MappingType.Date, "date", TIMESTAMP),
        Arguments.of(OpenSearchDataType.MappingType.Object, "object", STRUCT),
        Arguments.of(OpenSearchDataType.MappingType.Nested, "nested", ARRAY),
        Arguments.of(OpenSearchDataType.MappingType.GeoPoint, "geo_point",
                new OpenSearchGeoPointType()),
        Arguments.of(OpenSearchDataType.MappingType.Binary, "binary", new OpenSearchBinaryType()),
        Arguments.of(OpenSearchDataType.MappingType.Ip, "ip", new OpenSearchIpType())
    );
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("getTestDataWithType")
  public void ofType(OpenSearchDataType.MappingType mappingType, String name, ExprType dataType) {
    var type = OpenSearchDataType.of(mappingType);
    // For serialization of SQL and PPL different functions are used, and it was designed to return
    // different types. No clue why, but it should be fixed in #1296.
    var nameForSQL = name;
    var nameForPPL = name.equals("text") ? "string" : name;
    assertAll(
        () -> assertEquals(nameForPPL, type.typeName()),
        () -> assertEquals(nameForSQL, type.legacyTypeName()),
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
  @EnumSource(OpenSearchDataType.MappingType.class)
  public void ofOpenSearchDataTypeFromMappingType(OpenSearchDataType.MappingType mappingType) {
    assumeFalse(mappingType == Invalid);
    var type = OpenSearchDataType.of(mappingType);
    var derivedType = OpenSearchDataType.of(type);
    assertEquals(type, derivedType);
  }

  @Test
  // Test and type added for coverage only
  public void ofNullMappingType() {
    assertThrows(IllegalArgumentException.class, () -> OpenSearchDataType.of(Invalid));
  }

  @Test
  // cloneEmpty doesn't clone properties, but clones fields and other attributes
  public void cloneEmpty() {
    var type = new TestType(OpenSearchDataType.MappingType.Object,
        Map.of("val", OpenSearchDataType.of(INTEGER)),
        Map.of("words", OpenSearchDataType.of(STRING)));
    var clone = type.cloneEmpty();

    assertAll(
        // can compare because `properties` and `fields` are marked as @EqualsAndHashCode.Exclude
        () -> assertEquals(type, clone),
        // read private field `fields`
        () -> assertEquals(FieldUtils.readField(type, "fields", true),
            FieldUtils.readField(clone, "fields", true)),
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
    var flattened = OpenSearchDataType.traverseAndFlatten(getSampleMapping());
    var objectType = OpenSearchDataType.of(OpenSearchDataType.MappingType.Object);
    assertAll(
        () -> assertEquals(9, flattened.size()),
        () -> assertTrue(flattened.get("type").getProperties().isEmpty()),
        () -> assertTrue(flattened.get("type.subtype").getProperties().isEmpty()),
        () -> assertTrue(flattened.get("type.subtype.subsubtype").getProperties().isEmpty()),

        () -> assertEquals(objectType, flattened.get("type")),
        () -> assertEquals(objectType, flattened.get("type.subtype")),
        () -> assertEquals(objectType, flattened.get("type.subtype.subsubtype")),

        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword),
            flattened.get("type.keyword")),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Text),
            flattened.get("type.text")),

        () -> assertEquals(new OpenSearchGeoPointType(), flattened.get("type.subtype.geo_point")),
        () -> assertEquals(new OpenSearchTextType(),
            flattened.get("type.subtype.textWithFieldsType")),

        () -> assertEquals(new OpenSearchTextType(),
            flattened.get("type.subtype.subsubtype.textWithKeywordType")),
        () -> assertEquals(OpenSearchDataType.of(INTEGER),
            flattened.get("type.subtype.subsubtype.INTEGER"))
    );
  }

  @Test
  public void resolve() {
    var mapping = getSampleMapping();
    assertAll(
        () -> assertNull(OpenSearchDataType.resolve(mapping, "incorrect")),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Object),
            OpenSearchDataType.resolve(mapping, "type")),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Object),
            OpenSearchDataType.resolve(mapping, "subtype")),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Object),
            OpenSearchDataType.resolve(mapping, "subsubtype")),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Text),
            OpenSearchDataType.resolve(mapping, "textWithKeywordType")),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Text),
            OpenSearchDataType.resolve(mapping, "textWithFieldsType")),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Text),
            OpenSearchDataType.resolve(mapping, "text")),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Integer),
            OpenSearchDataType.resolve(mapping, "INTEGER")),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.GeoPoint),
            OpenSearchDataType.resolve(mapping, "geo_point")),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword),
            OpenSearchDataType.resolve(mapping, "keyword"))
    );
  }

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

  @Test
  public void textTypeWithFieldsCtor() {
    var type = new OpenSearchTextType(Map.of("words",
        OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)));
    assertAll(
        () -> assertEquals(new OpenSearchTextType(), type),
        () -> assertEquals(1, type.getFields().size()),
        () -> assertEquals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword),
            type.getFields().get("words"))
    );
  }

  private Map<String, OpenSearchDataType> getSampleMapping() {
    var textWithKeywordType = new OpenSearchTextType(Map.of("keyword",
        OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)));

    var subsubsubtypes = Map.of(
        "textWithKeywordType", textWithKeywordType,
        "INTEGER", OpenSearchDataType.of(INTEGER));

    var subsubtypes = Map.of(
        "subsubtype", OpenSearchDataType.of(OpenSearchDataType.MappingType.Object,
            subsubsubtypes, Map.of()),
        "textWithFieldsType", OpenSearchDataType.of(OpenSearchDataType.MappingType.Text, Map.of(),
            Map.of("words", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword))),
        "geo_point", new OpenSearchGeoPointType());

    var subtypes = Map.of(
        "subtype", OpenSearchDataType.of(OpenSearchDataType.MappingType.Object,
            subsubtypes, Map.of()),
        "keyword", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword),
        "text", OpenSearchDataType.of(OpenSearchDataType.MappingType.Text));

    var type = OpenSearchDataType.of(OpenSearchDataType.MappingType.Object, subtypes, Map.of());
    return Map.of("type", type);
  }

  private static class TestType extends OpenSearchDataType {
    public TestType(MappingType mappingType, Map<String, OpenSearchDataType> properties,
                    Map<String, OpenSearchDataType> fields) {
      this.exprCoreType = OpenSearchDataType.of(mappingType).exprCoreType;
      this.mappingType = mappingType;
      this.properties = properties;
      this.fields = fields;
    }

    public OpenSearchDataType cloneEmpty() {
      return super.cloneEmpty();
    }
  }
}
