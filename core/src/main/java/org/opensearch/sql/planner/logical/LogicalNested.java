/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
  private final List<Map<String, ReferenceExpression>> fields = new ArrayList<>();

  /**
   * Constructor of LogicalNested.
   */
  public LogicalNested(LogicalPlan childPlan, List<List<Expression>> nestedArgs) {
    super(Collections.singletonList(childPlan));
    for (var nested : nestedArgs) {
      if (nested.size() == 2) {
        this.fields.add(Map.of("field", (ReferenceExpression)nested.get(0),
            "path", (ReferenceExpression)nested.get(1)));
      } else {
        this.fields.add(Map.of("field", (ReferenceExpression)nested.get(0),
            "path", new ReferenceExpression(generatePathString(nested.get(0).toString()), STRING)));
      }
    }
  }

  private String generatePathString(String field) {
    return field.substring(0, field.lastIndexOf("."));
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
