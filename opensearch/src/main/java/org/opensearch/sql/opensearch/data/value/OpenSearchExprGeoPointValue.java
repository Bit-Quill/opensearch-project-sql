/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import java.util.LinkedHashMap;
import java.util.Map;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchGeoPointType;

/**
 * OpenSearch GeoPointValue.
 * Todo, add this to avoid the unknown value type exception, the implementation will be changed.
 */
public class OpenSearchExprGeoPointValue extends ExprTupleValue {

  /**
   * Constructor for OpenSearchExprGeoPointValue.
   * @param lat double value of latitude property of geo_point
   * @param lon double value of longitude property of geo_point
   */
  public OpenSearchExprGeoPointValue(Double lat, Double lon) {
    super(new LinkedHashMap<>(Map.of(
        "lat", (ExprValue) new ExprDoubleValue(lat),
        "lon", (ExprValue) new ExprDoubleValue(lon))));
  }

  @Override
  public ExprType type() {
    return OpenSearchGeoPointType.of();
  }
}
