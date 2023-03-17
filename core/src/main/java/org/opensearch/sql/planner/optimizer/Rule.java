/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import org.opensearch.sql.planner.logical.LogicalPlan;

/**
 * Optimization Rule.
 * @param <T> LogicalPlan.
 */
public interface Rule<T> {
  public enum blahEnum {
    RE_ITERATE,
    CONTINUE_ITERATE,
    TERMINATE_RULES
  }

  /**
   * Get the {@link Pattern}.
   */
  Pattern<T> pattern();

  /**
   * Apply the Rule to the LogicalPlan.
   * @param plan LogicalPlan which match the Pattern.
   * @param captures A list of LogicalPlan which are captured by the Pattern.
   * @return the transfromed LogicalPlan.
   */
  LogicalPlan apply(T plan, Captures captures);

  default blahEnum planRoute(LogicalPlan match, LogicalPlan node) {
    return (match != node) ? blahEnum.RE_ITERATE : blahEnum.CONTINUE_ITERATE;
  }
}
