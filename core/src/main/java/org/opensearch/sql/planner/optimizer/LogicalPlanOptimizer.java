/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer;

import static com.facebook.presto.matching.DefaultMatcher.DEFAULT_MATCHER;

import com.facebook.presto.matching.Match;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.rule.MergeFilterAndFilter;
import org.opensearch.sql.planner.optimizer.rule.PushFilterUnderSort;
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

  private final List<Pair<Rule<? extends LogicalPlan>, Boolean>> rules;

  /**
   * Create {@link LogicalPlanOptimizer} with pre-defined rules.
   */
  public static LogicalPlanOptimizer create() {
    // Boolean parameter - whether rule can be applied more than once.
    // TODO make it ^ a part of `Rule` interface?
    // Known restrictions:
    // 1) Limit the last
    return new LogicalPlanOptimizer(
        new ImmutableList.Builder<Pair<Rule<? extends LogicalPlan>, Boolean>>()
            /*
             * Phase 1: Transformations that rely on relational algebra equivalence
             */
            // TODO do we need MergeFilterAndFilter?
            //  OpenSearchRequestBuilder.pushDownFilter knows to merge conditions
            .add(Pair.of(new MergeFilterAndFilter(), true))
            // TODO do we need PushFilterUnderSort?
            .add(Pair.of(new PushFilterUnderSort(), true))
            /*
             * Phase 2: Transformations that rely on data source push down capability
             */
            .add(Pair.of(new CreateTableScanBuilder(), false))
            // Create enum for rule status - applied, not applied, etc
            .add(Pair.of(TableScanPushDown.PUSH_DOWN_PAGE_SIZE, false))
            .add(Pair.of(TableScanPushDown.PUSH_DOWN_FILTER, true))
            .add(Pair.of(TableScanPushDown.PUSH_DOWN_AGGREGATION, false))
            .add(Pair.of(TableScanPushDown.PUSH_DOWN_FILTER, true))
            .add(Pair.of(TableScanPushDown.PUSH_DOWN_SORT, true))
            .add(Pair.of(TableScanPushDown.PUSH_DOWN_HIGHLIGHT, true))
            .add(Pair.of(TableScanPushDown.PUSH_DOWN_NESTED, false))
            .add(Pair.of(TableScanPushDown.PUSH_DOWN_PROJECT, false))
            .add(Pair.of(TableScanPushDown.PUSH_DOWN_LIMIT, false))
            .add(Pair.of(new CreateTableWriteBuilder(), false))
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
