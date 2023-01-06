package org.opensearch.sql.common.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringUtilsTest {
  @Test
  public void test_getAliasWithNested() {
    String name = "nested(field.subfield)";
    assertEquals("field.subfield", StringUtils.getAliasWithNested(name));
  }

  @Test
  public void test_nested_in_function_getAliasWithNested() {
    String name = "sum(nested(field.subfield))";
    assertEquals("sum(field.subfield)", StringUtils.getAliasWithNested(name));
  }

  @Test
  public void test_multiple_nested_in_function_getAliasWithNested() {
    String name = "concat(nested(field.subfield), nested(field.subfield))";
    assertEquals("concat(field.subfield, field.subfield)", StringUtils.getAliasWithNested(name));
  }

  @Test
  public void test_nested_in_function_with_arguments_getAliasWithNested() {
    String name = "concat(nested(field.subfield), \"Add String\")";
    assertEquals("concat(field.subfield, \"Add String\")", StringUtils.getAliasWithNested(name));
  }
}
