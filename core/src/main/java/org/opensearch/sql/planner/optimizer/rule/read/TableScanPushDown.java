/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.rule.read;

import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalEval;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.logical.LogicalWindow;
import org.opensearch.sql.planner.optimizer.PushDownRule;
import org.opensearch.sql.planner.optimizer.Rule;

/**
 * Rule template for all table scan push down rules. Because all push down optimization rules
 * have similar workflow in common, such as a pattern that match an operator on top of table scan
 * builder, and action that eliminates the original operator if pushed down, this class helps
 * remove redundant code and improve readability.
 */
// TODO update comment or move rules and delete class
public class TableScanPushDown {

  /** Push down optimize rule for filtering condition. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_FILTER_DEEP =
      new PushDownRule<>(LogicalFilter.class).configureDeepTraverseRule(true,
          (filter, scanBuilder) -> scanBuilder.pushDownFilter(filter),
          (plan) -> plan instanceof LogicalAggregation,
          (plan) -> plan instanceof LogicalProject);
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_FILTER =
      new PushDownRule<>(LogicalFilter.class).configureRegularRule(
          (filter, scanBuilder) -> scanBuilder.pushDownFilter(filter));

  /** Push down optimize rule for aggregate operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_AGGREGATION_DEEP =
      new PushDownRule<>(LogicalAggregation.class).configureDeepTraverseRule(true,
          (agg, scanBuilder) -> scanBuilder.pushDownAggregation(agg),
          (plan) -> plan instanceof LogicalProject);
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_AGGREGATION =
      new PushDownRule<>(LogicalAggregation.class).configureRegularRule(
          (agg, scanBuilder) -> scanBuilder.pushDownAggregation(agg));

  /** Push down optimize rule for sort operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_SORT_DEEP =
      new PushDownRule<>(LogicalSort.class).configureDeepTraverseRule(true,
          (sort, scanBuilder) -> scanBuilder.pushDownSort(sort),
          (plan) -> plan instanceof LogicalProject);
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_SORT =
      new PushDownRule<>(LogicalSort.class).configureRegularRule(
          (sort, scanBuilder) -> scanBuilder.pushDownSort(sort));

  /** Push down optimize rule for limit operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_LIMIT_DEEP =
      new PushDownRule<>(LogicalLimit.class).configureDeepTraverseRule(false,
          (limit, scanBuilder) -> scanBuilder.pushDownLimit(limit),
          (plan) -> plan instanceof LogicalSort,
          (plan) -> plan instanceof LogicalFilter,
          (plan) -> plan instanceof LogicalProject);
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_LIMIT =
      new PushDownRule<>(LogicalLimit.class).configureRegularRule(
          (limit, scanBuilder) -> scanBuilder.pushDownLimit(limit));

  /** Push down optimize rule for Project operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_PROJECT_DEEP =
      new PushDownRule<>(LogicalProject.class).configureDeepTraverseRule(false,
          (project, scanBuilder) -> scanBuilder.pushDownProject(project),
          (plan) -> plan instanceof LogicalWindow);
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_PROJECT =
      new PushDownRule<>(LogicalProject.class).configureRegularRule(
          (project, scanBuilder) -> scanBuilder.pushDownProject(project));

  /** Push down optimize rule for highlight operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_HIGHLIGHT =
      new PushDownRule<>(LogicalHighlight.class).configureDeepTraverseRule(true,
          (highlight, scanBuilder) -> scanBuilder.pushDownHighlight(highlight));

  /** Push down optimize rule for nested operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_NESTED =
      new PushDownRule<>(LogicalNested.class).configureDeepTraverseRule(false,
          (nested, scanBuilder) -> scanBuilder.pushDownNested(nested));

  /** Push down optimize rule for paginate operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_PAGE_SIZE =
      new PushDownRule<>(LogicalPaginate.class).configureDeepTraverseRule(false,
          (paginate, scanBuilder) -> scanBuilder.pushDownPageSize(paginate));
}
