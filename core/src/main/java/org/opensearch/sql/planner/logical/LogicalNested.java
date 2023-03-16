/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;

@EqualsAndHashCode(callSuper = true)
@Getter
@ToString
public class LogicalNested extends LogicalPlan {
  private final List<Map<String, ReferenceExpression>> fields;
  /**
   * Project list is merged in from logical project at query optimization.
   */
  private final List<NamedExpression> projectList;

  /**
   * Constructor of LogicalNested.
   *
   */
  public LogicalNested(
      LogicalPlan childPlan,
      List<Map<String, ReferenceExpression>> fields,
      List<NamedExpression> projectList
  )
  {
    super(Collections.singletonList(childPlan));
    this.fields = fields;
    this.projectList = projectList;
  }

  public static String getFieldFromMap(Map<String, ReferenceExpression> map) {
    return map.get("field").toString();
  }

  public static String getPathFromMap(Map<String, ReferenceExpression> map) {
    return map.get("path").toString();
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitUnnest(this, context);
  }
}
