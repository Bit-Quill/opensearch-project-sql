/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import java.util.Map;
import java.util.Objects;
import lombok.Data;
import org.opensearch.sql.data.model.AbstractExprValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchGeoPointType;

/**
 * OpenSearch GeoPointValue.
 * Todo, add this to avoid the unknown value type exception, the implementation will be changed.
 */
public class OpenSearchExprGeoPointValue extends AbstractExprValue {

  private final GeoPoint geoPoint;

  public OpenSearchExprGeoPointValue(Double lat, Double lon) {
    this.geoPoint = new GeoPoint(lat, lon);
  }

  @Override
  public Object value() {
    return geoPoint;
  }

  @Override
  public ExprType type() {
    return OpenSearchGeoPointType.of();
  }

  @Override
  public int compare(ExprValue other) {
    return geoPoint.toString()
        .compareTo((((OpenSearchExprGeoPointValue) other).geoPoint).toString());
  }

  @Override
  public boolean equal(ExprValue other) {
    Map<String, ExprValue> otherTupleValue = other.tupleValue();
    return geoPoint.equals(new OpenSearchExprGeoPointValue(
        other.tupleValue().get("lat").doubleValue(),
        other.tupleValue().get("lon").doubleValue()).geoPoint);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(geoPoint);
  }

  @Data
  public static class GeoPoint {

    private final Double lat;

    private final Double lon;
  }
}
