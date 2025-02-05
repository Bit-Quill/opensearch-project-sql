/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.FunctionExpression;

@ExtendWith(MockitoExtension.class)
public class JsonFunctionsTest {
  private static final ExprValue JsonNestedObject =
      ExprValueUtils.stringValue("{\"a\":\"1\",\"b\":{\"c\":\"2\",\"d\":\"3\"}}");
  private static final ExprValue JsonObject =
      ExprValueUtils.stringValue("{\"a\":\"1\",\"b\":\"2\"}");
  private static final ExprValue JsonArray = ExprValueUtils.stringValue("[1, 2, 3, 4]");
  private static final ExprValue JsonScalarString = ExprValueUtils.stringValue("\"abc\"");
  private static final ExprValue JsonEmptyString = ExprValueUtils.stringValue("");
  private static final ExprValue JsonInvalidObject =
      ExprValueUtils.stringValue("{\"invalid\":\"json\", \"string\"}");
  private static final ExprValue JsonInvalidScalar = ExprValueUtils.stringValue("abc");

  @Test
  public void json_valid_returns_false() {
    assertEquals(LITERAL_FALSE, execute(JsonInvalidObject));
    assertEquals(LITERAL_FALSE, execute(JsonInvalidScalar));
    assertEquals(LITERAL_FALSE, execute(LITERAL_NULL));
    assertEquals(LITERAL_FALSE, execute(LITERAL_MISSING));
  }

  @Test
  public void json_valid_throws_ExpressionEvaluationException() {
    assertThrows(
        ExpressionEvaluationException.class, () -> execute(ExprValueUtils.booleanValue(true)));
  }

  @Test
  public void json_valid_returns_true() {
    assertEquals(LITERAL_TRUE, execute(JsonNestedObject));
    assertEquals(LITERAL_TRUE, execute(JsonObject));
    assertEquals(LITERAL_TRUE, execute(JsonArray));
    assertEquals(LITERAL_TRUE, execute(JsonScalarString));
    assertEquals(LITERAL_TRUE, execute(JsonEmptyString));
  }

  private ExprValue execute(ExprValue jsonString) {
    FunctionExpression exp = DSL.jsonValid(DSL.literal(jsonString));
    return exp.valueOf();
  }

  @Test
  void json_returnsJsonObject() {
    FunctionExpression exp;

    // Setup
    final String objectJson =
        "{\"foo\": \"foo\", \"fuzz\": true, \"bar\": 1234, \"bar2\": 12.34, \"baz\": null, "
            + "\"obj\": {\"internal\": \"value\"}, \"arr\": [\"string\", true, null]}";

    LinkedHashMap<String, ExprValue> objectMap = new LinkedHashMap<>();
    objectMap.put("foo", new ExprStringValue("foo"));
    objectMap.put("fuzz", ExprBooleanValue.of(true));
    objectMap.put("bar", new ExprLongValue(1234));
    objectMap.put("bar2", new ExprDoubleValue(12.34));
    objectMap.put("baz", ExprNullValue.of());
    objectMap.put(
        "obj", ExprTupleValue.fromExprValueMap(Map.of("internal", new ExprStringValue("value"))));
    objectMap.put(
        "arr",
        new ExprCollectionValue(
            List.of(new ExprStringValue("string"), ExprBooleanValue.of(true), ExprNullValue.of())));
    ExprValue expectedTupleExpr = ExprTupleValue.fromExprValueMap(objectMap);

    // exercise
    exp = DSL.json_function(DSL.literal(objectJson));

    // Verify
    var value = exp.valueOf();
    assertTrue(value instanceof ExprTupleValue);
    assertEquals(expectedTupleExpr, value);
  }

  @Test
  void json_returnsJsonArray() {
    FunctionExpression exp;

    // Setup
    final String arrayJson = "[\"foo\", \"fuzz\", true, \"bar\", 1234, 12.34, null]";
    ExprValue expectedArrayExpr =
        new ExprCollectionValue(
            List.of(
                new ExprStringValue("foo"),
                new ExprStringValue("fuzz"),
                LITERAL_TRUE,
                new ExprStringValue("bar"),
                new ExprIntegerValue(1234),
                new ExprDoubleValue(12.34),
                LITERAL_NULL));

    // exercise
    exp = DSL.json_function(DSL.literal(arrayJson));

    // Verify
    var value = exp.valueOf();
    assertTrue(value instanceof ExprCollectionValue);
    assertEquals(expectedArrayExpr, value);
  }

  @Test
  void json_returnsScalar() {
    assertEquals(
        new ExprStringValue("foobar"), DSL.json_function(DSL.literal("\"foobar\"")).valueOf());

    assertEquals(new ExprIntegerValue(1234), DSL.json_function(DSL.literal("1234")).valueOf());

    assertEquals(LITERAL_TRUE, DSL.json_function(DSL.literal("true")).valueOf());

    assertEquals(LITERAL_NULL, DSL.json_function(DSL.literal("null")).valueOf());

    assertEquals(LITERAL_NULL, DSL.json_function(DSL.literal("")).valueOf());

    assertEquals(
        ExprTupleValue.fromExprValueMap(Map.of()), DSL.json_function(DSL.literal("{}")).valueOf());
  }

  @Test
  void json_returnsSemanticCheckException() {
    // invalid type
    assertThrows(
        SemanticCheckException.class, () -> DSL.castJson(DSL.literal("invalid")).valueOf());

    // missing bracket
    assertThrows(SemanticCheckException.class, () -> DSL.castJson(DSL.literal("{{[}}")).valueOf());

    // mnissing quote
    assertThrows(
        SemanticCheckException.class, () -> DSL.castJson(DSL.literal("\"missing quote")).valueOf());
  }

  @Test
  void json_set_InsertByte() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"), DSL.literal((byte) 'a'));
    assertEquals("[97]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertShort() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"), DSL.literal(Short.valueOf("123")));
    assertEquals("[123]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertInt() {
    FunctionExpression functionExpression =
        DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"), DSL.literal(123));
    assertEquals("[123]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertLong() {
    FunctionExpression functionExpression =
        DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"), DSL.literal(123L));
    assertEquals("[123]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertFloat() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"), DSL.literal(123.123F));
    assertEquals("[123.123]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertDouble() {
    FunctionExpression functionExpression =
        DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"), DSL.literal(123.123));
    assertEquals("[123.123]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertString() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"), DSL.literal("test_value"));
    assertEquals("[\"test_value\"]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertBoolean() {
    FunctionExpression functionExpression =
        DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"), DSL.literal(Boolean.TRUE));
    assertEquals("[true]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertDate() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"),
                    DSL.date(DSL.literal(new ExprDateValue("2020-08-17"))));
    assertEquals("[\"2020-08-17\"]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertTime() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"),
                    DSL.time(DSL.literal(new ExprTimeValue("01:01:01"))));
    assertEquals("[\"01:01:01\"]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertTimestamp() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"),
                    DSL.timestamp(DSL.literal("2008-05-15 22:00:00")));
    assertEquals("[\"2008-05-15 22:00:00\"]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertInterval() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"),
                    DSL.interval(DSL.literal(1), DSL.literal("second")));
    assertEquals("[{\"seconds\":1}]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertIp() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("[]"), DSL.literal("$"),
                    DSL.castIp(DSL.literal("192.168.1.1")));
    assertEquals("[\"192.168.1.1\"]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertList() {
    FunctionExpression functionExpression =
        DSL.jsonSet(
            DSL.literal("[{\"name\" : \"ben\"}]"),
            DSL.literal("$"),
            DSL.literal(
                new ExprCollectionValue(
                    List.of(new ExprStringValue("123"), new ExprStringValue("456")))));
    assertEquals(
        "[{\"name\":\"ben\"},[\"123\",\"456\"]]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertMap() {
    FunctionExpression functionExpression =
        DSL.jsonSet(
            DSL.literal("[{\"name\" : \"ben\"}]"),
            DSL.literal("$"),
            DSL.literal(
                ExprTupleValue.fromExprValueMap(Map.of("name", new ExprStringValue("alice")))));
    assertEquals(
        "[{\"name\":\"ben\"},{\"name\":\"alice\"}]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertArray() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("[]"),
                    DSL.literal("$"),
                    DSL.literal(
                            new ExprCollectionValue(List.of(
                                    new ExprStringValue("Alice"),
                                    new ExprStringValue("Ben")
                            ))));
    assertEquals(
            "[[\"Alice\",\"Ben\"]]", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_insert_new_arr_element() {
    FunctionExpression functionExpression =
        DSL.jsonSet(
            DSL.literal("{\"members\": []}"),
            DSL.literal("$.members"),
            DSL.literal(
                ExprTupleValue.fromExprValueMap(Map.of("name", new ExprStringValue("alice")))));
    assertEquals(
        "{\"members\":[{\"name\":\"alice\"}]}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_update_singleProperty() {
    FunctionExpression functionExpression =
        DSL.jsonSet(
            DSL.literal("{\"members\":[{\"name\":\"alice\"}]}"),
            DSL.literal("$.members[0].name"),
            DSL.literal("andy"));
    assertEquals("{\"members\":[{\"name\":\"andy\"}]}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_update_multipleProperties() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{\"members\":[{\"name\":\"alice\"}, {\"name\":\"ben\"}]}"),
                    DSL.literal("$.members..name"),
                    DSL.literal("andy"));
    assertEquals("{\"members\":[{\"name\":\"andy\"},{\"name\":\"andy\"}]}",
            functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_insert_new_property() {
    FunctionExpression functionExpression =
        DSL.jsonSet(
            DSL.literal("{\"members\":[{\"name\":\"alice\"}]}"),
            DSL.literal("$.members[0].age"),
            DSL.literal("18"));
    assertEquals(
        "{\"members\":[{\"name\":\"alice\",\"age\":\"18\"}]}",
        functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_insert_multiple_new_properties() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{\"members\":[{\"name\":\"alice\"},{\"name\":\"ben\"}]}"),
                    DSL.literal("$.members..age"),
                    DSL.literal("18"));
    assertEquals(
            "{\"members\":[{\"name\":\"alice\",\"age\":\"18\"}]}",
            functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_insert_new_property_nested() {
    FunctionExpression functionExpression =
        DSL.jsonSet(
            DSL.literal("{\"members\":[{\"name\":\"alice\"}]}"),
            DSL.literal("$.members[0].age.innerAge"),
            DSL.literal("18"));
    assertEquals(
        "{\"members\":[{\"name\":\"alice\",\"age\":{\"innerAge\":\"18\"}}]}",
        functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_insert_invalid_path() {
    FunctionExpression functionExpression =
        DSL.jsonSet(
            DSL.literal("{\"members\":[{\"name\":\"alice\"}]}"),
            DSL.literal("$$$$$$$$$"),
            DSL.literal("18"));

    assertEquals(LITERAL_NULL, functionExpression.valueOf());
  }


  @Test
  void json_set_noMatch_property() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{\"members\":[{\"name\":\"alice\"}]}"),
                    DSL.literal("$.members[0].age.innerAge"),
                    DSL.literal("18"));
    assertEquals(
            "{\"members\":[{\"name\":\"alice\",\"age\":{\"innerAge\":\"18\"}}]}",
            functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_noMatch_array() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{\"members\":[{\"name\":\"alice\"}]}"),
                    DSL.literal("$.members[0].age.innerArray"),
                    DSL.literal(
                            new ExprCollectionValue(List.of(
                                    new ExprStringValue("18"),
                                    new ExprStringValue("20")
                            ))));
    assertEquals(
            "{\"members\":[{\"name\":\"alice\",\"age\":{\"innerArray\":[\"18\",\"20\"]}}]}",
            functionExpression.valueOf().stringValue());
  }


  @Test
  void json_set_singleMatch_property() {

  }

  @Test
  void json_set_singleMatch_array() {

  }

  @Test
  void json_set_multiMatches_property() {

  }

  @Test
  void json_set_multiMatches_array() {

  }



}
