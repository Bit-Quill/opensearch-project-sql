/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
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
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;

@ExtendWith(MockitoExtension.class)
public class JsonFunctionsTest {
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
  }

  @Test
  public void json_valid_returns_true() {
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
  public void json_object_returns_tuple() {
    FunctionExpression exp;

    // Setup
    LinkedHashMap<String, ExprValue> objectMap = new LinkedHashMap<>();
    objectMap.put("foo", new ExprStringValue("foo"));
    objectMap.put("fuzz", ExprBooleanValue.of(true));
    objectMap.put("bar", new ExprLongValue(1234));
    objectMap.put("bar2", new ExprDoubleValue(12.34));
    objectMap.put("baz", ExprNullValue.of());
    objectMap.put(
        "obj", ExprTupleValue.fromExprValueMap(Map.of("internal", new ExprStringValue("value"))));
    // TODO: requires json_array()
    //    objectMap.put(
    //        "arr",
    //        new ExprCollectionValue(
    //            List.of(new ExprStringValue("string"), ExprBooleanValue.of(true), ExprNullValue.of())));
    ExprValue expectedTupleExpr = ExprTupleValue.fromExprValueMap(objectMap);

    // exercise
    exp = DSL.jsonObject(
        DSL.literal("foo"), DSL.literal("foo"),
        DSL.literal("fuzz"), DSL.literal(true),
        DSL.literal("bar"), DSL.literal(1234),
        DSL.literal("bar2"), DSL.literal(12.34),
        DSL.literal("baz"), new LiteralExpression(ExprValueUtils.nullValue()),
        DSL.literal("obj"), DSL.jsonObject(
            DSL.literal("internal"), DSL.literal("value")
        )
    );

    // Verify
    var value = exp.valueOf();
    assertTrue(value instanceof ExprTupleValue);
    assertEquals(expectedTupleExpr, value);
  }

  @Test
  public void json_object_returns_empty_tuple() {
    FunctionExpression exp;

    // Setup
    LinkedHashMap<String, ExprValue> objectMap = new LinkedHashMap<>();
    ExprValue expectedTupleExpr = ExprTupleValue.fromExprValueMap(objectMap);

    // exercise
    exp = DSL.jsonObject();

    // Verify
    var value = exp.valueOf();
    assertTrue(value instanceof ExprTupleValue);
    assertEquals(expectedTupleExpr, value);
  }

  @Test
  public void json_object_throws_SemanticCheckException() {
    // wrong number of arguments
    assertThrows(
        SemanticCheckException.class, () -> DSL.jsonObject(DSL.literal("only one")).valueOf());
    assertThrows(
        SemanticCheckException.class, () -> DSL.jsonObject(DSL.literal("one"), DSL.literal("two"), DSL.literal("three")).valueOf());

    // key argument is not a string
    assertThrows(
        SemanticCheckException.class, () -> DSL.jsonObject(DSL.literal(1234), DSL.literal("two")).valueOf());
    assertThrows(
        SemanticCheckException.class, () -> DSL.jsonObject(DSL.literal("one"), DSL.literal(true), DSL.literal(true), DSL.literal("four")).valueOf());
  }
}
