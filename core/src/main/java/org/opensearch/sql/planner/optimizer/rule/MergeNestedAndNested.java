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
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.Rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Merge Filter --> Filter to the single Filter condition.
 */
public class MergeNestedAndNested implements Rule<LogicalNested> {

  private final Capture<LogicalNested> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalNested> pattern;

  /**
   * Constructor of MergeFilterAndFilter.
   */
  public MergeNestedAndNested() {
    this.capture = Capture.newCapture();
    this.pattern = typeOf(LogicalNested.class)
        .with(source().matching(typeOf(LogicalNested.class).capturedAs(capture)));
  }

  @Override
  public LogicalPlan apply(LogicalNested filter,
      Captures captures) {
    LogicalNested childFilter = captures.get(capture);

    List<Map<String, ReferenceExpression>> combinedArgs = new ArrayList<>();
    combinedArgs.addAll(filter.getFields());
    combinedArgs.addAll(childFilter.getFields());

//    if (filter.getProjectList() != null) {
//      return new LogicalNested(
//          childFilter.getChild().get(0),
//          combinedArgs, filter.getProjectList());
//    } else {
      return new LogicalNested(
          childFilter.getChild().get(0),
          combinedArgs, null);
//    }
  }

  @Override
  public blahEnum planRoute(LogicalPlan match, LogicalPlan node) {
    return (match != node) ? blahEnum.TERMINATE_RULES : blahEnum.CONTINUE_ITERATE;
  }
}
