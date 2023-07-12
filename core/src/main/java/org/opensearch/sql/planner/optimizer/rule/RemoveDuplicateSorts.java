/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.optimizer.Rule;

/** TODO */
public class RemoveDuplicateSorts implements Rule<LogicalSort> {

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalSort> pattern;

  public RemoveDuplicateSorts() {
    // TODO rework do detect duplicates of any type and then update pattern if possible
    this.pattern = typeOf(LogicalSort.class).with(source().matching(typeOf(LogicalSort.class)));
  }

  @Override
  public LogicalPlan apply(LogicalSort plan, Captures captures) {
    // If plan has only one child, and it is _completely_ same as the plan
    if (plan.getChild().size() == 1
        // Can't do `plan.equals(child)` because `@EqualsAndHashCode` has `(callSuper = true)`
        && plan.getSortList().equals(((LogicalSort) plan.getChild().get(0)).getSortList())) {
      // remove child from the tree
      plan.replaceChildPlans(plan.getChild().get(0).getChild());
    }
    return plan;
  }
}
