/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer;

import static com.facebook.presto.matching.DefaultMatcher.DEFAULT_MATCHER;

import com.facebook.presto.matching.Match;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

// TODO add doc or merge with LogicalPlanOptimizer
public class LogicalPlanOptimizerVisitor extends LogicalPlanNodeVisitor<LogicalPlan, Void> {

  private final List<Rule<? extends LogicalPlan>> rules;
  private Rule<LogicalPlan> currentRule;
  private boolean currentRuleApplied = false;

  // TODO rename var
  private final List<Pair<Rule<? extends LogicalPlan>, LogicalPlan>> log = new ArrayList<>();

  public LogicalPlanOptimizerVisitor(List<Rule<? extends LogicalPlan>> rules) {
    this.rules = rules;
  }

  public LogicalPlan optimize(LogicalPlan planTree) {
    log(planTree);
    var node = planTree;
    for (int i = 0; i < rules.size(); i++) {
      // TODO how to avoid unchecked cast
      currentRule = (Rule<LogicalPlan>) rules.get(i);
      currentRuleApplied = false;
      node = node.accept(this, null);
      if (currentRuleApplied && currentRule.canBeAppliedMultipleTimes()) {
        // only for rules which could be applied multiple times
        // retry the rule in the i-th position
        i--;
      }
    }
    log(node);
    return node;
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

  private boolean isCurrentRuleAppliedToNode(LogicalPlan node) {
    for (var logEntry : log) {
      // A rule could be applied to the exactly equal, but not same tree node
      // We can't do `log.contains(Pair.of(currentRule, plan))` check
      if (logEntry.getLeft() == currentRule && logEntry.getRight() == node) {
        return true;
      }
    }
    return false;
  }

  @Override
  public LogicalPlan visitNode(LogicalPlan plan, Void noContext) {
    LogicalPlan node = plan;
    Match<? extends LogicalPlan> match = DEFAULT_MATCHER.match(currentRule.pattern(), node);
    if (!isCurrentRuleAppliedToNode(plan) && match.isPresent()) {
      currentRuleApplied = true;
      node = currentRule.apply(match.value(), match.captures());
      if (node != plan) {
        log.clear();
      }
      log.add(Pair.of(currentRule, plan));

      // For new TableScanPushDown impl, pattern match doesn't necessarily cause
      // push down to happen. So reiterate all rules against the node only if the node
      // is actually replaced by any rule.
      // TODO: may need to introduce fixed point or maximum iteration limit in future
    } else {
      node.replaceChildPlans(node.getChild().stream()
          .map(child -> child.accept(this, noContext))
          .collect(Collectors.toList()));
    }
    return node;
  }
}
