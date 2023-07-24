/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Rule which traverses the plan tree until:
 * <ul>
 *   <li>
 *     An exception satisfied; the rule matches nothing then.
 *   </li>
 *   <li>
 *     A tree node of the same type is found; the rule matches nothing then.
 *   </li>
 *   <li>
 *     An instance of {@link TableScanBuilder} found; rule matches the given plan node
 *     and can apply given {@link #pushDownFunction}.
 *   </li>
 * </ul>
 * If no {@link TableScanBuilder} found, the rule matches nothing.
 *
 * @param <T>
 */
public class PushDownRule<T extends LogicalPlan> extends Rule<T> {

  private final Class<T> clazz;
  private BiFunction<T, TableScanBuilder, Boolean> pushDownFunction;
  private List<Function<LogicalPlan, Boolean>> exceptions;
  private boolean deepTreeTraverse;

  public PushDownRule(Class<T> clazz) {
    this.clazz = clazz;
  }

  @SafeVarargs
  public final PushDownRule<T> configureDeepTraverseRule(
          boolean canBeAppliedMultipleTimes,
          BiFunction<T, TableScanBuilder, Boolean> pushDownFunction,
          Function<LogicalPlan, Boolean>... exceptions) {
    this.canBeAppliedMultipleTimes = canBeAppliedMultipleTimes;
    this.deepTreeTraverse = true;
    this.pushDownFunction = pushDownFunction;
    this.exceptions = new ArrayList<>(Arrays.asList(exceptions));
    this.exceptions.add(0, getDefaultException());
    return this;
  }

  public final PushDownRule<T> configureRegularRule(
          BiFunction<T, TableScanBuilder, Boolean> pushDownFunction) {
    this.canBeAppliedMultipleTimes = true;
    this.deepTreeTraverse = false;
    this.pushDownFunction = pushDownFunction;
    this.exceptions = List.of();
    return this;
  }

  /**
   * Default exception applicable for all rules: don't apply the rule if there is another instance
   * of same {@link #clazz} located down the tree.
   */
  private Function<LogicalPlan, Boolean> getDefaultException() {
    return (plan) -> plan.getClass().equals(clazz);
  }

  @Override
  public Pattern<T> pattern() {
    return Pattern.typeOf(clazz).matching(node -> findTableScanBuilder(node).isPresent());
  }

  @Override
  public LogicalPlan apply(T plan, Captures captures) {
    var builder = findTableScanBuilder(plan).orElseThrow();
    if (!pushDownFunction.apply(plan, builder)) {
      return plan;
    }
    return plan.getChild().get(0);
  }

  private Optional<TableScanBuilder> findTableScanBuilder(LogicalPlan node) {
    Deque<LogicalPlan> plans = new ArrayDeque<>();
    plans.add(node);
    do {
      var plan = plans.removeFirst();
      var children = plan.getChild();
      for (var exception : exceptions) {
        if (children.stream().anyMatch(exception::apply)) {
          return Optional.empty();
        }
      }
      if (children.stream().anyMatch(TableScanBuilder.class::isInstance)) {
        if (children.size() > 1) {
          throw new UnsupportedOperationException(
            "Unsupported plan: relation operator cannot have siblings");
        }
        return Optional.of((TableScanBuilder) children.get(0));
      }
      if (deepTreeTraverse) {
        plans.addAll(children);
      }
    } while (!plans.isEmpty());
    return Optional.empty();
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", getClass().getSimpleName(), clazz.getSimpleName());
  }
}
