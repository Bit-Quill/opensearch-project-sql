/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
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
    return new LogicalPlanOptimizer(
        new ImmutableList.Builder<Rule<? extends LogicalPlan>>()
            /*
             * Phase 1: Transformations that rely on data source push down capability
             */
            .add(new CreateTableScanBuilder())
            .add(TableScanPushDown.PUSH_DOWN_PAGE_SIZE)
            .add(TableScanPushDown.PUSH_DOWN_FILTER)
            .add(TableScanPushDown.PUSH_DOWN_AGGREGATION)
            .add(TableScanPushDown.PUSH_DOWN_FILTER)
            .add(TableScanPushDown.PUSH_DOWN_SORT)
            .add(TableScanPushDown.PUSH_DOWN_HIGHLIGHT)
            .add(TableScanPushDown.PUSH_DOWN_NESTED)
            .add(TableScanPushDown.PUSH_DOWN_PROJECT)
            .add(TableScanPushDown.PUSH_DOWN_LIMIT)
            .add(new CreateTableWriteBuilder())
            .build()
    );
  }

  /**
   * Optimize {@link LogicalPlan}.
   */
  public LogicalPlan optimize(LogicalPlan plan) {
    return new LogicalPlanOptimizerVisitor(rules).optimize(plan);
  }
}
