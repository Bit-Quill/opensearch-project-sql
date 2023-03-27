/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

@EqualsAndHashCode(callSuper = true)
@Getter
@ToString
public class LogicalNested extends LogicalPlan {
  private final List<Map<String, ReferenceExpression>> fields;
  private final List<NamedExpression> projectList;

  /**
   * Constructor of LogicalNested.
   *
   */
  public LogicalNested(
      LogicalPlan childPlan,
      List<Map<String, ReferenceExpression>> fields,
      List<NamedExpression> projectList
  ) {
    super(Collections.singletonList(childPlan));
    this.fields = fields;
    this.projectList = projectList;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitUnnest(this, context);
  }

  /**
   * Map all field names in nested queries that use same path.
   * @return : Map of path and associated field names.
   */
  public Map<String, List<String>> groupFieldNamesByPath() {
    return this.fields.stream().collect(
        Collectors.groupingBy(
            m -> m.get("path").toString(),
            mapping(
                m -> m.get("field").toString(),
                toList()
            )
        )
    );
  }
}
