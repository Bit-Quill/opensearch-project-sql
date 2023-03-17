/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;

import static org.opensearch.sql.data.model.ExprValueUtils.jsonify;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.UNDEFINED;

/**
 * Physical operator for Values.
 */
@ToString
@EqualsAndHashCode(callSuper = false, of = "values")
public class ValuesOperator extends PhysicalPlan {

  /**
   * Original values list for print and equality check.
   */
  @Getter
  private final List<List<LiteralExpression>> values;

  /**
   * Values iterator.
   */
  private final Iterator<List<LiteralExpression>> valuesIterator;

  public ValuesOperator(List<List<LiteralExpression>> values) {
    this.values = values;
    this.valuesIterator = values.iterator();
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitValues(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return ImmutableList.of();
  }

  @Override
  public boolean hasNext() {
    return valuesIterator.hasNext();
  }

  @Override
  public long getTotalHits() {
    // ValuesOperator used for queries without `FROM` clause, e.g. `select 1`.
    // Such query always returns 1 row.
    return 1;
  }

  @Override
  public ExprValue next() {
    List<ExprValue> values = valuesIterator.next().stream()
                                           .map(Expression::valueOf)
                                           .collect(Collectors.toList());
    return new ExprCollectionValue(values);
  }
}
