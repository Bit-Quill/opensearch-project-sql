/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer;

import static com.facebook.presto.matching.DefaultMatcher.DEFAULT_MATCHER;

import com.facebook.presto.matching.Match;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

public class LogicalPlanOptimizerVisitor extends LogicalPlanNodeVisitor<LogicalPlan, Void> {

  private final List<Pair<Rule<? extends LogicalPlan>, Boolean>> rules;
  private final Queue<Pair<Rule<? extends LogicalPlan>, Boolean>> queue;
  private Rule<LogicalPlan> currentRule;
  private boolean currentRuleApplied = false;
  private boolean anyRuleApplied = false;

  private final List<Pair<Rule<? extends LogicalPlan>, Integer>> log = new ArrayList<>();

  public LogicalPlanOptimizerVisitor(List<Pair<Rule<? extends LogicalPlan>, Boolean>> rules) {
    this.rules = rules;//new LinkedList<>(rules);
    queue = new ArrayDeque<>(rules);
  }

  public LogicalPlan optimize(LogicalPlan planTree) {
    var node = planTree;
    //do {
      anyRuleApplied = false;
      for (int i = 0; i < rules.size(); i++) {
        var ruleConfig = rules.get(i);
        currentRule = (Rule<LogicalPlan>) ruleConfig.getKey();
        currentRuleApplied = false;
        node = node.accept(this, null);
        if (currentRuleApplied && ruleConfig.getValue()) {
          // To re-try the rule in the i-th position
          // only for rules which could be applied multiple times
          i--;
        }
      }
    //} while (anyRuleApplied);
    return node;
  }

  public String log(LogicalPlan plan, int depth) {
    return String.format("%" + depth + "s", plan.getClass().getSimpleName())
        + (plan.getChild().size() == 0 ? "" : "\n" + log(plan.getChild().get(0), depth + 1));
  }


  @Override
  public LogicalPlan visitNode(LogicalPlan plan, Void noContext) {
    LogicalPlan node = plan;
    Match<? extends LogicalPlan> match = DEFAULT_MATCHER.match(currentRule.pattern(), node);
    if (!log.contains(Pair.of(currentRule, System.identityHashCode(plan))) && match.isPresent()) {
      anyRuleApplied = currentRuleApplied = true;
      node = currentRule.apply(match.value(), match.captures());
      if (node != plan) {
        log.clear();
      }
      log.add(Pair.of(currentRule, System.identityHashCode(plan)));

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
