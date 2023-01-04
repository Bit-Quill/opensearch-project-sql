/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.Type.Keyword;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.Type.Text;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
}
