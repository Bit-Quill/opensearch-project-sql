/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer;

import static com.facebook.presto.matching.DefaultMatcher.DEFAULT_MATCHER;

import com.facebook.presto.matching.Match;
import java.util.List;
import java.util.stream.Collectors;
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
 * TODO update comment ^
 */
@RequiredArgsConstructor
public class LogicalPlanOptimizer {

  private final List<Rule<? extends LogicalPlan>> rules;

  private final OptimizingMode optimizingMode;

  // TODO comment
  public enum OptimizingMode {
    PRESERVE_TREE_ORDER,
    PRESERVE_RULE_ORDER
  }

  /**
   * Create {@link LogicalPlanOptimizer} with pre-defined rules.
   * TODO comment
   */
  public static LogicalPlanOptimizer create() {
    return new LogicalPlanOptimizer(List.of(
        /*
         * Phase 1: Transformations that rely on data source push down capability
         */
        new CreateTableScanBuilder(),
        TableScanPushDown.PUSH_DOWN_PAGE_SIZE,
        TableScanPushDown.PUSH_DOWN_FILTER_DEEP,
        TableScanPushDown.PUSH_DOWN_AGGREGATION_DEEP,
        TableScanPushDown.PUSH_DOWN_FILTER_DEEP,
        TableScanPushDown.PUSH_DOWN_SORT_DEEP,
        TableScanPushDown.PUSH_DOWN_HIGHLIGHT,
        TableScanPushDown.PUSH_DOWN_NESTED,
        TableScanPushDown.PUSH_DOWN_PROJECT_DEEP,
        TableScanPushDown.PUSH_DOWN_LIMIT_DEEP,
        new CreateTableWriteBuilder()
    ), OptimizingMode.PRESERVE_RULE_ORDER);
  }

  /**
   * TODO comment
   * TODO rename
   */
  public static LogicalPlanOptimizer create2() {
    return new LogicalPlanOptimizer(List.of(
        new CreateTableScanBuilder(),
        TableScanPushDown.PUSH_DOWN_PAGE_SIZE,
        TableScanPushDown.PUSH_DOWN_FILTER,
        TableScanPushDown.PUSH_DOWN_AGGREGATION,
        TableScanPushDown.PUSH_DOWN_SORT,
        TableScanPushDown.PUSH_DOWN_LIMIT,
        TableScanPushDown.PUSH_DOWN_HIGHLIGHT,
        TableScanPushDown.PUSH_DOWN_NESTED,
        TableScanPushDown.PUSH_DOWN_PROJECT,
        new CreateTableWriteBuilder()
    ), OptimizingMode.PRESERVE_TREE_ORDER);
  }

  /**
   * Optimize {@link LogicalPlan}.
   */
  public LogicalPlan optimize(LogicalPlan plan) {
    log(plan);
    var optimized = plan;
    if (optimizingMode == OptimizingMode.PRESERVE_RULE_ORDER) {
      optimized = new LogicalPlanOptimizerVisitor(rules).optimize(plan);
    } else { //PRESERVE_TREE_ORDER
      optimized = optimize2(plan);
    }
    log(optimized);
    return optimized;
  }


  // TODO remove debugging
  public void log(LogicalPlan plan) {
    var node = plan;
    System.out.println("==============");
    System.out.println(node.getClass().getSimpleName());
    while (node.getChild().size() > 0) {
      node = node.getChild().get(0);
      System.out.println("      |");
      System.out.println(node.getClass().getSimpleName());
    }
    System.out.println("==============");
  }

  // TODO merge classes, reuse code and make rename functions

  /**
   * Optimize {@link LogicalPlan}.
   */
  public LogicalPlan optimize2(LogicalPlan plan) {
    LogicalPlan optimized = internalOptimize(plan);
    optimized.replaceChildPlans(
        optimized.getChild().stream().map(this::optimize2).collect(
            Collectors.toList()));
    return internalOptimize(optimized);
  }

  private LogicalPlan internalOptimize(LogicalPlan plan) {
    LogicalPlan node = plan;
    boolean done = false;
    while (!done) {
      done = true;
      for (Rule rule : rules) {
        Match match = DEFAULT_MATCHER.match(rule.pattern(), node);
        if (match.isPresent()) {
          node = rule.apply(match.value(), match.captures());

          // For new TableScanPushDown impl, pattern match doesn't necessarily cause
          // push down to happen. So reiterate all rules against the node only if the node
          // is actually replaced by any rule.
          // TODO: may need to introduce fixed point or maximum iteration limit in future
          if (node != match.value()) {
            done = false;
          }
        }
      }
    }
    return node;
  }
}
