/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.optimizer.Rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Merge Filter --> Filter to the single Filter condition.
 */
public class MergeProjectAndNested implements Rule<LogicalProject> {

  private final Capture<LogicalNested> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalProject> pattern;

  /**
   * Constructor of MergeFilterAndFilter.
   */
  public MergeProjectAndNested() {
    this.capture = Capture.newCapture();
    this.pattern = typeOf(LogicalProject.class)
        .with(source().matching(typeOf(LogicalNested.class).capturedAs(capture)));
  }

  @Override
  public LogicalPlan apply(LogicalProject filter,
      Captures captures) {
    LogicalNested childFilter = captures.get(capture);

    // TODO should push nested under project
    if (childFilter.getFields() != null) {
      return new LogicalProject(
          new LogicalNested(childFilter.getChild().get(0),
              childFilter.getFields(), filter.getProjectList()),
          filter.getProjectList(),
          filter.getNamedParseExpressions()
      );
//      return new LogicalNested(
//          childFilter.getChild().get(0),
//          childFilter.getFields(), filter.getProjectList());
//      return filter.replaceChildPlans(
//          List.of(new LogicalNested(
//          childFilter.getChild().get(0),
//          childFilter.getFields(), filter.getProjectList()))
//      );
    } else {
      return filter;
    }
  }
  @Override
  public blahEnum planRoute(LogicalPlan match, LogicalPlan node) {
    return (match != node) ? blahEnum.TERMINATE_RULES : blahEnum.CONTINUE_ITERATE;
  }
}
