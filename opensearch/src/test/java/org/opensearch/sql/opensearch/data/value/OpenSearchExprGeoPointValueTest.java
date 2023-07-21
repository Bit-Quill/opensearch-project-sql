/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchGeoPointType;

class OpenSearchExprGeoPointValueTest {

  private OpenSearchExprGeoPointValue geoPointValue = new OpenSearchExprGeoPointValue(1.0, 1.0);

  @Test
  void type() {
    assertEquals(OpenSearchGeoPointType.of(), geoPointValue.type());
  }
}
