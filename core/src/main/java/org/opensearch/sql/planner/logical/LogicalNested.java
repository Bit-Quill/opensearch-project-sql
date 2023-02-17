/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

@EqualsAndHashCode(callSuper = true)
@Getter
@ToString
public class LogicalNested extends LogicalPlan {
  private final ReferenceExpression field;
  private final ReferenceExpression path;

  /**
   * Constructor of LogicalNested.
   */
  public LogicalNested(LogicalPlan childPlan, List<Expression> nestedArgs) {
    super(Collections.singletonList(childPlan));
    if (nestedArgs.size() == 2) {
      field = (ReferenceExpression)nestedArgs.get(0);
      path = (ReferenceExpression)nestedArgs.get(1);
    } else {
      field = (ReferenceExpression)nestedArgs.get(0);
      path = new ReferenceExpression(generatePathString(field.toString()), STRING);
    }
  }

  private String generatePathString(String field) {
    return field.substring(0, field.lastIndexOf("."));
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitUnnest(this, context);
  }
}
