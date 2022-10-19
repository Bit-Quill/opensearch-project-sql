/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.utils;

import static org.opensearch.sql.data.model.ExprValueUtils.getDoubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getFloatValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getIntegerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getLongValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getStringValue;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.time.LocalDate;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class ComparisonUtil {
  /**
   * Util to compare the object (integer, long, float, double, string) values.
   * ExprValue A
   */
  public static int compare(ExprValue v1, ExprValue v2) {
    if (v1.isMissing() || v2.isMissing()) {
      throw new ExpressionEvaluationException("invalid to call compare operation on missing value");
    } else if (v1.isNull() || v2.isNull()) {
      throw new ExpressionEvaluationException("invalid to call compare operation on null value");
    } else if (v1.type() != v2.type() && List.of(DATE, TIME, DATETIME, TIMESTAMP)
          .containsAll(List.of((ExprCoreType)v1.type(), (ExprCoreType)v2.type()))) {
      return v1.datetimeValue().compareTo(v2.datetimeValue());
    } else if (v1.type() != v2.type()) {
      throw new ExpressionEvaluationException(
          "invalid to call compare operation on values of different types");
    }

    switch ((ExprCoreType)v1.type()) {
      case BYTE: return v1.byteValue().compareTo(v2.byteValue());
      case SHORT: return v1.shortValue().compareTo(v2.shortValue());
      case INTEGER: return getIntegerValue(v1).compareTo(getIntegerValue(v2));
      case LONG: return getLongValue(v1).compareTo(getLongValue(v2));
      case FLOAT: return getFloatValue(v1).compareTo(getFloatValue(v2));
      case DOUBLE: return getDoubleValue(v1).compareTo(getDoubleValue(v2));
      case STRING: return getStringValue(v1).compareTo(getStringValue(v2));
      case BOOLEAN: return v1.booleanValue().compareTo(v2.booleanValue());
      case TIME: return v1.timeValue().compareTo(v2.timeValue());
      case DATE: return v1.dateValue().compareTo(v2.dateValue());
      case DATETIME: return v1.datetimeValue().compareTo(v2.datetimeValue());
      case TIMESTAMP: return v1.timestampValue().compareTo(v2.timestampValue());
      default: throw new ExpressionEvaluationException(
          String.format("%s instances are not comparable", v1.getClass().getSimpleName()));
    }
  }
}
