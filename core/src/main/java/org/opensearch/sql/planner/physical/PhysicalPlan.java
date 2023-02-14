/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import java.util.Iterator;
import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.planner.PlanNode;
import org.opensearch.sql.storage.split.Split;

/**
 * Physical plan.
 */
public abstract class PhysicalPlan implements PlanNode<PhysicalPlan>,
    Iterator<ExprValue>,
    AutoCloseable {

  public static final List<String> FORBIDDEN_CHARS = List.of("(", ")", ",");

  /**
   * Accept the {@link PhysicalPlanNodeVisitor}.
   *
   * @param visitor visitor.
   * @param context visitor context.
   * @param <R>     returned object type.
   * @param <C>     context type.
   * @return returned object.
   */
  public abstract <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context);

  public void open() {
    getChild().forEach(PhysicalPlan::open);
  }

  public void close() {
    getChild().forEach(PhysicalPlan::close);
  }

  public void add(Split split) {
    getChild().forEach(child -> child.add(split));
  }

  public ExecutionEngine.Schema schema() {
    throw new IllegalStateException(String.format("[BUG] schema can been only applied to "
        + "ProjectOperator, instead of %s", this.getClass().getSimpleName()));
  }

  public String toCursor() {
    throw new IllegalStateException(String.format("%s is not compatible with cursor feature",
        this.getClass().getSimpleName()));
  }

  /**
   * Creates an S-expression that represents a plan node.
   * @param plan Label for the plan.
   * @param params List of serialized parameters. Including the child plans.
   * @return A string that represents the plan called with those parameters.
   */
  protected String createSection(String plan, String... params) {
    if (FORBIDDEN_CHARS.stream().anyMatch(plan::contains)) {
      var error = String.format("plan key '%s' contains forbidden character",
          plan);
      throw new RuntimeException(error);
    }

    // TODO: check that each param is either a valid s-expression or
    // does not contain forbidden characters.
    return "(" + plan + ","
        + String.join(",", params)
        + ")";
  }
}
