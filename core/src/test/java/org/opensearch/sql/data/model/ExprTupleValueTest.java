/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.utils.ComparisonUtil.compare;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

import java.time.Period;
import java.util.LinkedHashMap;
import java.util.Map;

class ExprTupleValueTest {
  @Test
  public void equal_to_itself() {
    ExprValue tupleValue = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    assertTrue(tupleValue.equals(tupleValue));
  }

  @Test
  public void tuple_compare_int() {
    ExprValue tupleValue = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    ExprValue intValue = ExprValueUtils.integerValue(10);
    assertFalse(tupleValue.equals(intValue));
  }

  @Test
  public void compare_tuple_with_different_key() {
    ExprValue tupleValue1 = ExprValueUtils.tupleValue(ImmutableMap.of("value", 2));
    ExprValue tupleValue2 =
        ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2, "float_value", 1f));
    assertNotEquals(tupleValue1, tupleValue2);
    assertNotEquals(tupleValue2, tupleValue1);
  }

  @Test
  public void compare_tuple_with_different_size() {
    ExprValue tupleValue1 = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    ExprValue tupleValue2 =
        ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2, "float_value", 1f));
    assertFalse(tupleValue1.equals(tupleValue2));
    assertFalse(tupleValue2.equals(tupleValue1));
  }

  @Test
  public void comparabilityTest() {
    ExprValue value1 = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    ExprValue value2 =
        ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2, "float_value", 1f));
    assertEquals(0, compare(value1, value1));
    assertEquals(0, compare(value2, value2));
    assertEquals(1, compare(value1, value2));
    assertEquals(1, compare(value2, value1));
  }

  @Test
  public void value() {
    ExprValue value =
        ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2, "float_value", 1f));
    assertEquals(new LinkedHashMap<>(ImmutableMap.of("integer_value", 2, "float_value", 1f)),
        value.value());
  }

  @Test
  public void type() {
    ExprValue tuple = ExprValueUtils.tupleValue(Map.of());
    assertEquals(STRUCT, tuple.type());
  }
}
