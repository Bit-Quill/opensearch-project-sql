/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class IndexMappingTest {

  @Test
  public void getFieldType() {
    IndexMapping indexMapping = new IndexMapping(ImmutableMap.of("name", "text"));
    assertEquals("text", indexMapping.getFieldType("name"));
    assertNull(indexMapping.getFieldType("not_exist"));
  }
}
