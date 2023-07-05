/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer;

import static com.facebook.presto.matching.DefaultMatcher.DEFAULT_MATCHER;

import com.facebook.presto.matching.Match;

import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

public class LogicalPlanOptimizerVisitor extends LogicalPlanNodeVisitor<LogicalPlan, Void> {

  private final List<Pair<Rule<? extends LogicalPlan>, Boolean>> rules;
  private final Queue<Rule<? extends LogicalPlan>> queue;
  private Rule<LogicalPlan> currentRule;
  private boolean currentRuleApplied = false;
  private boolean anyRuleApplied = false;

  public LogicalPlanOptimizerVisitor(List<Pair<Rule<? extends LogicalPlan>, Boolean>> rules) {
    this.rules = new LinkedList<>(rules);
    queue = rules.stream().map(Pair::getLeft).collect(Collectors.toCollection(ArrayDeque::new));
  }

  public LogicalPlan optimize(LogicalPlan planTree) {
    var node = planTree;
    do {
      anyRuleApplied = false;
      for (int i = 0; i < rules.size(); i++) {
        var ruleConfig = rules.get(i);
        currentRule = (Rule<LogicalPlan>) ruleConfig.getKey();
        currentRuleApplied = false;
        node = node.accept(this, null);
        if (currentRuleApplied && !ruleConfig.getValue()) {
          // a rule which could be used only once was applied
          rules.remove(i);
          i--;
        }
      }
    } while (anyRuleApplied);
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
    if (match.isPresent()) {
      anyRuleApplied = currentRuleApplied = true;
      node = currentRule.apply(match.value(), match.captures());

      // For new TableScanPushDown impl, pattern match doesn't necessarily cause
      // push down to happen. So reiterate all rules against the node only if the node
      // is actually replaced by any rule.
      // TODO: may need to introduce fixed point or maximum iteration limit in future
      {
        if (!queue.contains(currentRule)) {
          throw new RuntimeException("pewpew");
        }
        Rule<? extends LogicalPlan> rule = null;
        while (!currentRule.equals(rule)) {
          rule = queue.poll();
        }
      }
    } else {
      node.replaceChildPlans(node.getChild().stream()
          .map(child -> child.accept(this, noContext))
          .collect(Collectors.toList()));
    }
    return node;
  }
}
