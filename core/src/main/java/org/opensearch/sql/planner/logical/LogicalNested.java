/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.expression.Expression;

@EqualsAndHashCode(callSuper = true)
@Getter
@ToString
public class LogicalNested extends LogicalPlan {
  private final Expression field;

  /**
   * Constructor of LogicalNested.
   */
  public LogicalNested(LogicalPlan childPlan, Expression field) {
    super(Collections.singletonList(childPlan));
    this.field = field;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNested(this, context);
  }
}
