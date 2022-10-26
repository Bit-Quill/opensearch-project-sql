/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.List;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * The average aggregator aggregate the value evaluated by the expression.
 * If the expression evaluated result is NULL or MISSING, then the result is NULL.
 */
public class AvgAggregator extends Aggregator<AvgAggregator.AvgState> {

  /**
   * To process by different ways different data types, we need to store the type.
   * Input data has the same type as the result.
   */
  private final ExprCoreType dataType;

  public AvgAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.AVG.getName(), arguments, returnType);
    dataType = returnType;
  }

  @Override
  public AvgState create() {
    switch (dataType) {
      case DATE:
      case DATETIME:
      case TIMESTAMP:
      case TIME:
        return new DateTimeAvgState(dataType);
      case DOUBLE:
        return new DoubleAvgState();
      default: //unreachable code - we don't expose signatures for unsupported types
        throw new IllegalArgumentException(
            String.format("avg aggregation over %s type is not supported", dataType));
    }
  }

  @Override
  protected AvgState iterate(ExprValue value, AvgState state) {
    return state.iterate(value);
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "avg(%s)", format(getArguments()));
  }

  /**
   * Average State.
   */
  protected abstract static class AvgState implements AggregationState {
    protected int count;
    protected double total;

    AvgState() {
      this.count = 0;
      this.total = 0d;
    }

    @Override
    public abstract ExprValue result();

    protected AvgState iterate(ExprValue value) {
      count++;
      return this;
    }
  }

  protected static class DoubleAvgState extends AvgState {
    @Override
    public ExprValue result() {
      if (count == 0) {
        return ExprNullValue.of();
      }
      return ExprValueUtils.doubleValue(total / count);
    }

    @Override
    protected AvgState iterate(ExprValue value) {
      total += ExprValueUtils.getDoubleValue(value);
      return super.iterate(value);
    }
  }

  @RequiredArgsConstructor
  protected static class DateTimeAvgState extends AvgState {
    private final ExprCoreType dataType;

    @Override
    public ExprValue result() {
      if (count == 0) {
        return ExprNullValue.of();
      }
      return ExprValueUtils.convertEpochMilliToDateTimeType(Math.round(total / count), dataType);
    }

    @Override
    protected AvgState iterate(ExprValue value) {
      total += ExprValueUtils.extractEpochMilliFromAnyDateTimeType(value);
      return super.iterate(value);
    }
  }
}
