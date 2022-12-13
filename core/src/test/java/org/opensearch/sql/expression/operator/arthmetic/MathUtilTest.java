package org.opensearch.sql.expression.operator.arthmetic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.expression.operator.arthmetic.MathUtil.truncateDouble;
import static org.opensearch.sql.expression.operator.arthmetic.MathUtil.truncateFloat;
import static org.opensearch.sql.expression.operator.arthmetic.MathUtil.truncateInt;
import static org.opensearch.sql.expression.operator.arthmetic.MathUtil.truncateLong;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class for {@link MathUtil}.
 */
class MathUtilTest {

  @ParameterizedTest
  @ValueSource(doubles = {11.2D, 22.5678D, -1.2D})
  void testTruncateDouble(final Double value) {
    String result = Double.toString(truncateDouble(value, 1));
    assertEquals(Double.toString(value).substring(0,4), result);
  }

  @ParameterizedTest(name = "truncate({0}, {1})")
  @ValueSource(floats = {11.2F, 22.5678F, -1.2F})
  void testTruncateFloat(final Float value) {
    String result = Double.toString(truncateFloat(value, 1));
    assertEquals(Float.toString(value).substring(0,4), result);
  }

  @ParameterizedTest
  @ValueSource(longs = {2056L, -777L})
  void testTruncateLong(final Long value) {
    String result = Long.toString(truncateLong(value, 1));
    assertEquals(Long.toString(value).substring(0,4), result);
  }

  @ParameterizedTest
  @ValueSource(ints = {11, 22, -7})
  void testTruncateInt(final int value) {
    String result = Long.toString(truncateInt(value, 1));
    assertEquals(Integer.toString(value).substring(0,2), result);
  }
}