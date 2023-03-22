/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.operator.predicate.BinaryPredicateOperator;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

/**
 * The Filter operator represents WHERE clause and
 * uses the conditions to evaluate the input {@link BindingTuple}.
 * The Filter operator only returns the results that evaluated to true.
 * The NULL and MISSING are handled by the logic defined in {@link BinaryPredicateOperator}.
 */
@EqualsAndHashCode(callSuper = false)
@ToString
@RequiredArgsConstructor
public class FilterOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final Expression conditions;
  @ToString.Exclude
  private ExprValue next = null;
  private long totalHits = 0;

  // A copy constructor
  public FilterOperator(PhysicalPlan input, FilterOperator other) {
    this.input = input;
    this.conditions = other.conditions;
    this.next = other.next;
    this.totalHits = other.totalHits;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitFilter(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    if (next != null) {
      return true;
    }
    while (input.hasNext()) {
      ExprValue inputValue = input.next();
      ExprValue exprValue = conditions.valueOf(inputValue.bindingTuples());
      if (!(exprValue.isNull() || exprValue.isMissing()) && (exprValue.booleanValue())) {
        next = inputValue;
        totalHits++;
        return true;
      }
    }
    return false;
  }

  @Override
  public ExprValue next() {
    var res = next;
    next = null;
    return res;
  }

  @Override
  public long getTotalHits() {
    // ignore `input.getTotalHits()`, because it returns wrong (unfiltered) value
    return totalHits;
  }

  // Probably we don't need this, because we do pushDownFilter in OpenSearchPagedIndexScanBuilder
  @Override
  public boolean writeExternal(ObjectOutput out) throws IOException {
    PlanLoader loader = (in, engine) -> {
      var conditions = (Expression) in.readObject();
      var next = (ExprValue) in.readObject();
      var totalHits = in.readLong();
      var inputLoader = (PlanLoader) in.readObject();
      var input = (PhysicalPlan) inputLoader.apply(in, engine);
      var fo = new FilterOperator(input, conditions);
      fo.next = next;
      fo.totalHits = totalHits;
      return fo;
    };
    out.writeObject(loader);

    out.writeObject(conditions);
    out.writeObject(next);
    out.writeLong(totalHits);
    return input.getPlanForSerialization().writeExternal(out);
  }
}
