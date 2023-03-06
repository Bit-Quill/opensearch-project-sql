/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.parse.ParseExpression;

/**
 * Project the fields specified in {@link ProjectOperator#projectList} from input.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class ProjectOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final List<NamedExpression> projectList;
  @Getter
  private final List<NamedExpression> namedParseExpressions;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitProject(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  private String handleDuplicateColumns(String newName, Set takenNames) {
    if (takenNames.contains(newName)) {
      int appendedNum = 1;
      while (takenNames.contains(String.format("%s%d", newName, appendedNum))) {
        appendedNum++;
      }
      newName = String.format("%s%d", newName, appendedNum);
    }
    takenNames.add(newName);
    return newName;
  }

  @Override
  public ExprValue next() {
    ExprValue inputValue = input.next();
    ImmutableMap.Builder<String, ExprValue> mapBuilder = new Builder<>();
    Set<String> columns = new HashSet<>();

    // ParseExpression will always override NamedExpression when identifier conflicts
    // TODO needs a better implementation, see https://github.com/opensearch-project/sql/issues/458
    for (NamedExpression expr : projectList) {
      ExprValue exprValue = expr.valueOf(inputValue.bindingTuples());
      Optional<NamedExpression> optionalParseExpression = namedParseExpressions.stream()
          .filter(parseExpr -> parseExpr.getNameOrAlias().equals(expr.getNameOrAlias()))
          .findFirst();

      if (optionalParseExpression.isEmpty()) {
        mapBuilder.put(handleDuplicateColumns(expr.getNameOrAlias(), columns), exprValue);
        continue;
      }

      NamedExpression parseExpression = optionalParseExpression.get();
      ExprValue sourceFieldValue = inputValue.bindingTuples()
          .resolve(((ParseExpression) parseExpression.getDelegated()).getSourceField());
      String columnName = handleDuplicateColumns(parseExpression.getNameOrAlias(), columns);
      if (sourceFieldValue.isMissing()) {
        // source field will be missing after stats command, read from inputValue if it exists
        // otherwise do nothing since it should not appear as a field
        ExprValue tupleValue =
            ExprValueUtils.getTupleValue(inputValue).get(columnName);
        if (tupleValue != null) {
          mapBuilder.put(columnName, tupleValue);
        }
      } else {
        ExprValue parsedValue = parseExpression.valueOf(inputValue.bindingTuples());
        mapBuilder.put(columnName, parsedValue);
      }
    }
    return ExprTupleValue.fromExprValueMap(mapBuilder.build());
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return new ExecutionEngine.Schema(getProjectList().stream()
        .map(expr -> new ExecutionEngine.Schema.Column(expr.getName(),
            expr.getAlias(), expr.type())).collect(Collectors.toList()));
  }
}
