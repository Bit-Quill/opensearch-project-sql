/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
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
public class ProjectOperator extends PhysicalPlan {
  @Getter
  private PhysicalPlan input;
  @Getter
  private List<NamedExpression> projectList;
  @Getter
  private List<NamedExpression> namedParseExpressions;

  public ProjectOperator() {
    int a = 5;
    // TODO validate that called only from deserializer
  }

  public ProjectOperator(PhysicalPlan input, List<NamedExpression> projectList,
                         List<NamedExpression> namedParseExpressions) {
    this.input = input;
    this.projectList = projectList;
    this.namedParseExpressions = namedParseExpressions;
  }

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

  @Override
  public ExprValue next() {
    ExprValue inputValue = input.next();
    ImmutableMap.Builder<String, ExprValue> mapBuilder = new Builder<>();

    // ParseExpression will always override NamedExpression when identifier conflicts
    // TODO needs a better implementation, see https://github.com/opensearch-project/sql/issues/458
    for (NamedExpression expr : projectList) {
      ExprValue exprValue = expr.valueOf(inputValue.bindingTuples());
      Optional<NamedExpression> optionalParseExpression = namedParseExpressions.stream()
          .filter(parseExpr -> parseExpr.getNameOrAlias().equals(expr.getNameOrAlias()))
          .findFirst();
      if (optionalParseExpression.isEmpty()) {
        mapBuilder.put(expr.getNameOrAlias(), exprValue);
        continue;
      }

      NamedExpression parseExpression = optionalParseExpression.get();
      ExprValue sourceFieldValue = inputValue.bindingTuples()
          .resolve(((ParseExpression) parseExpression.getDelegated()).getSourceField());
      if (sourceFieldValue.isMissing()) {
        // source field will be missing after stats command, read from inputValue if it exists
        // otherwise do nothing since it should not appear as a field
        ExprValue tupleValue =
            ExprValueUtils.getTupleValue(inputValue).get(parseExpression.getNameOrAlias());
        if (tupleValue != null) {
          mapBuilder.put(parseExpression.getNameOrAlias(), tupleValue);
        }
      } else {
        ExprValue parsedValue = parseExpression.valueOf(inputValue.bindingTuples());
        mapBuilder.put(parseExpression.getNameOrAlias(), parsedValue);
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

  @SuppressWarnings("unchecked")
  @Override
  public boolean writeExternal(ObjectOutput out) throws IOException {
    PlanLoader loader = (in, engine) -> {
      var projectList = (List<NamedExpression>) in.readObject();
      var namedParseExpressions = (List<NamedExpression>) in.readObject();
      var inputLoader = (PlanLoader) in.readObject();
      var input = (PhysicalPlan) inputLoader.apply(in, engine);
      return new ProjectOperator(input, projectList, namedParseExpressions);
    };
    out.writeObject(loader);

    out.writeObject(projectList);
    out.writeObject(namedParseExpressions);
    return input.getPlanForSerialization().writeExternal(out);
  }
}
