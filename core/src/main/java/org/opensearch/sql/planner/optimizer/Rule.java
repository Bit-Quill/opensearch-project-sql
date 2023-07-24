/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.planner.logical.LogicalPlan;

/**
 * Optimization Rule.
 * @param <T> LogicalPlan.
 */
public abstract class Rule<T> {

  @Accessors(fluent = true)
  @Getter
  protected boolean canBeAppliedMultipleTimes;

  /**
   * Get the {@link Pattern}.
   */
  public abstract Pattern<T> pattern();

  /**
   * Apply the Rule to the LogicalPlan.
   * @param plan LogicalPlan which match the Pattern.
   * @param captures A list of LogicalPlan which are captured by the Pattern.
   * @return the transformed LogicalPlan.
   */
  public abstract LogicalPlan apply(T plan, Captures captures);

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
