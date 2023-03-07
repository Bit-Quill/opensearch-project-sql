/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.nested;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.floatValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

public class NestedFunctionTest extends ExpressionTestBase {
  protected static Environment<Expression, ExprValue> env() {
    return var -> {
      if (var instanceof ReferenceExpression) {
        switch (((ReferenceExpression) var).getAttr()) {
          case "message.integer_value":
            return integerValue(1);
          case "message.long_value":
            return longValue(1L);
          case "message.float_value":
            return floatValue(1f);
          case "message.double_value":
            return doubleValue(1d);
          case "message.boolean_value":
            return booleanValue(true);
          case "message.string_value":
            return stringValue("str");
          case "message":
          case "message.struct_value":
            return tupleValue(ImmutableMap.of("str", 1));
          case "message.array_value":
            return collectionValue(ImmutableList.of(1));
          default:
            throw new IllegalArgumentException("undefined reference");
        }
      } else {
        throw new IllegalArgumentException("var must be ReferenceExpression");
      }
    };
  }


  @Test
  public void test_nested_string_val() {
    FunctionExpression nested_str = DSL.nested(DSL.ref("message.string_value", STRING));
    assertEquals(STRING, nested_str.type());
    assertEquals("str", ExprValueUtils.getStringValue(nested_str.valueOf(env())));
    assertEquals("nested(message.string_value)", nested_str.toString());
  }

  @Test
  public void test_nested_integer_val() {
    FunctionExpression nested_int = DSL.nested(DSL.ref("message.integer_value", STRING));
    assertEquals(STRING, nested_int.type());
    assertEquals(1, ExprValueUtils.getIntegerValue(nested_int.valueOf(env())));
    assertEquals("nested(message.integer_value)", nested_int.toString());
  }

  @Test
  public void test_nested_long_val() {
    FunctionExpression nested_long = DSL.nested(DSL.ref("message.long_value", STRING));
    assertEquals(STRING, nested_long.type());
    assertEquals(1L, ExprValueUtils.getLongValue(nested_long.valueOf(env())));
    assertEquals("nested(message.long_value)", nested_long.toString());
  }

  @Test
  public void test_nested_float_val() {
    FunctionExpression nested_float = DSL.nested(DSL.ref("message.float_value", STRING));
    assertEquals(STRING, nested_float.type());
    assertEquals(1f, ExprValueUtils.getFloatValue(nested_float.valueOf(env())));
    assertEquals("nested(message.float_value)", nested_float.toString());
  }

  @Test
  public void test_nested_double_val() {
    FunctionExpression nested_double = DSL.nested(DSL.ref("message.double_value", STRING));
    assertEquals(STRING, nested_double.type());
    assertEquals(1d, ExprValueUtils.getDoubleValue(nested_double.valueOf(env())));
    assertEquals("nested(message.double_value)", nested_double.toString());
  }

  @Test
  public void test_nested_boolean_val() {
    FunctionExpression nested_boolean = DSL.nested(DSL.ref("message.boolean_value", STRING));
    assertEquals(STRING, nested_boolean.type());
    assertEquals(true, ExprValueUtils.getBooleanValue(nested_boolean.valueOf(env())));
    assertEquals("nested(message.boolean_value)", nested_boolean.toString());
  }

  @Test
  public void test_nested_struct_val() {
    FunctionExpression nested_struct = DSL.nested(DSL.ref("message.struct_value", STRING));
    assertEquals(STRING, nested_struct.type());
    assertEquals(integerValue(1), ExprValueUtils.getTupleValue(nested_struct.valueOf(env())).get("str"));
    assertEquals("nested(message.struct_value)", nested_struct.toString());
  }

  @Test
  public void test_nested_array_val() {
    FunctionExpression nested_array = DSL.nested(DSL.ref("message.array_value", STRING));
    assertEquals(STRING, nested_array.type());
    assertEquals(integerValue(1), ExprValueUtils.getCollectionValue(nested_array.valueOf(env())).get(0));
    assertEquals("nested(message.array_value)", nested_array.toString());
  }

  @Test
  public void test_nested_array_with_double_param() {
    FunctionExpression nested_array = DSL.nested(DSL.ref("message.array_value", STRING),
        DSL.ref("message", ARRAY));
    assertEquals(STRING, nested_array.type());
    assertEquals(integerValue(1), ExprValueUtils.getCollectionValue(nested_array.valueOf(env())).get(0));
    assertEquals("nested(message.array_value, message)", nested_array.toString());
  }

  @Test
  public void test_nested_struct_with_double_param() {
    FunctionExpression nested_struct = DSL.nested(DSL.ref("message.struct_value", STRING),
        DSL.ref("message", STRUCT));
    assertEquals(STRING, nested_struct.type());
    assertEquals(integerValue(1), ExprValueUtils.getTupleValue(nested_struct.valueOf(env())).get("str"));
    assertEquals("nested(message.struct_value, message)", nested_struct.toString());
  }
}
