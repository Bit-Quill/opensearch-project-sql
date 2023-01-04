/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

class OpenSearchExprTextValueTest {
  @Test
  public void typeOfExprTextValue() {
    assertEquals(new OpenSearchTextType(), new OpenSearchExprTextValue("A").type());
  }
}
