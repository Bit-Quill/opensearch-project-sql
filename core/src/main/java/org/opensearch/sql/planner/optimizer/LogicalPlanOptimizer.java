/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.rule.read.CreateTableScanBuilder;
import org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown;
import org.opensearch.sql.planner.optimizer.rule.write.CreateTableWriteBuilder;

/**
 * {@link LogicalPlan} Optimizer.
 * The Optimizer will run in the TopDown manner.
 * 1> Optimize the current node with all the rules.
 * 2> Optimize the all the child nodes with all the rules.
 * 3) In case the child node could change, Optimize the current node again.
 */
@RequiredArgsConstructor
public class LogicalPlanOptimizer {

  private final List<Rule<? extends LogicalPlan>> rules;

  /**
   * Create {@link LogicalPlanOptimizer} with pre-defined rules.
   */
  public static LogicalPlanOptimizer create() {
    return new LogicalPlanOptimizer(List.of(
        /*
         * Phase 1: Transformations that rely on data source push down capability
         */
        new CreateTableScanBuilder(),
        TableScanPushDown.PUSH_DOWN_PAGE_SIZE,
        TableScanPushDown.PUSH_DOWN_FILTER,
        TableScanPushDown.PUSH_DOWN_AGGREGATION,
        TableScanPushDown.PUSH_DOWN_FILTER,
        TableScanPushDown.PUSH_DOWN_SORT,
        TableScanPushDown.PUSH_DOWN_HIGHLIGHT,
        TableScanPushDown.PUSH_DOWN_NESTED,
        TableScanPushDown.PUSH_DOWN_PROJECT,
        TableScanPushDown.PUSH_DOWN_LIMIT,
        new CreateTableWriteBuilder()
    ));
  }

  /**
   * Optimize {@link LogicalPlan}.
   */
  public LogicalPlan optimize(LogicalPlan plan) {
    return new LogicalPlanOptimizerVisitor(rules).optimize(plan);
  }
}
