/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.rule.read;

import static org.opensearch.sql.planner.optimizer.pattern.Patterns.aggregate;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.filter;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.highlight;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.limit;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.nested;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.project;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.scanBuilder;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.sort;
import static org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown.TableScanPushDownBuilder.match;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.matching.pattern.CapturePattern;
import com.facebook.presto.matching.pattern.WithPattern;
import java.util.function.BiFunction;

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
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Rule template for all table scan push down rules. Because all push down optimization rules
 * have similar workflow in common, such as a pattern that match an operator on top of table scan
 * builder, and action that eliminates the original operator if pushed down, this class helps
 * remove redundant code and improve readability.
 *
 * @param <T> logical plan node type
 */
public class TableScanPushDown<T extends LogicalPlan> implements Rule<T> {

  /** Push down optimize rule for filtering condition. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_FILTER =
      new PushDownRule<>(LogicalFilter.class,
          (filter, scanBuilder) -> scanBuilder.pushDownFilter(filter),
          (plan) -> plan instanceof LogicalAggregation || plan instanceof LogicalProject);

  /** Push down optimize rule for aggregate operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_AGGREGATION =
      new PushDownRule<>(LogicalAggregation.class,
          (agg, scanBuilder) -> scanBuilder.pushDownAggregation(agg),
          (plan) -> plan instanceof LogicalProject);

  /** Push down optimize rule for sort operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_SORT =
      new PushDownRule<>(LogicalSort.class,
          (sort, scanBuilder) -> scanBuilder.pushDownSort(sort),
          (plan) -> plan instanceof LogicalProject);

  /** Push down optimize rule for limit operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_LIMIT =
      new PushDownRule<>(LogicalLimit.class,
          (limit, scanBuilder) -> scanBuilder.pushDownLimit(limit),
          (plan) -> plan instanceof LogicalSort || plan instanceof LogicalFilter
                  || plan instanceof LogicalProject);

  /** Push down optimize rule for Project operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_PROJECT =
      new PushDownRule<>(LogicalProject.class,
          (project, scanBuilder) -> scanBuilder.pushDownProject(project),
          (plan) -> plan instanceof LogicalEval || plan instanceof LogicalWindow);

  /** Push down optimize rule for highlight operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_HIGHLIGHT =
      new PushDownRule<>(LogicalHighlight.class,
          (highlight, scanBuilder) -> scanBuilder.pushDownHighlight(highlight));

  /** Push down optimize rule for nested operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_NESTED =
      new PushDownRule<>(LogicalNested.class,
          (nested, scanBuilder) -> scanBuilder.pushDownNested(nested));

  /** Push down optimize rule for paginate operator. */
  public static final Rule<? extends LogicalPlan> PUSH_DOWN_PAGE_SIZE =
      new PushDownRule<>(LogicalPaginate.class,
          (paginate, scanBuilder) -> scanBuilder.pushDownPageSize(paginate));

  /** Pattern that matches a plan node. */
  private final WithPattern<T> pattern;

  /** Capture table scan builder inside a plan node. */
  private final Capture<TableScanBuilder> capture;

  /** Push down function applied to the plan node and captured table scan builder. */
  private final BiFunction<T, TableScanBuilder, Boolean> pushDownFunction;


  @SuppressWarnings("unchecked")
  private TableScanPushDown(WithPattern<T> pattern,
                            BiFunction<T, TableScanBuilder, Boolean> pushDownFunction) {
    this.pattern = pattern;
    this.capture = ((CapturePattern<TableScanBuilder>) pattern.getPattern()).capture();
    this.pushDownFunction = pushDownFunction;
  }

  @Override
  public Pattern<T> pattern() {
    return pattern;
  }

  @Override
  public String toString() {
    return pattern.toString().split("\n")[0];
  }

  @Override
  public LogicalPlan apply(T plan, Captures captures) {
    TableScanBuilder scanBuilder = captures.get(capture);
    if (pushDownFunction.apply(plan, scanBuilder)) {
      return scanBuilder;
    }
    return plan;
  }

  /**
   * Custom builder class other than generated by Lombok to provide more readable code.
   */
  static class TableScanPushDownBuilder<T extends LogicalPlan> {

    private WithPattern<T> pattern;

    public static <T extends LogicalPlan>
        TableScanPushDownBuilder<T> match(Pattern<T> pattern) {
      TableScanPushDownBuilder<T> builder = new TableScanPushDownBuilder<>();
      builder.pattern = (WithPattern<T>) pattern;
      return builder;
    }

    public TableScanPushDown<T> apply(
        BiFunction<T, TableScanBuilder, Boolean> pushDownFunction) {
      return new TableScanPushDown<>(pattern, pushDownFunction);
    }
  }
}
