/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.ObjectOutput;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
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
      var projectsStr = in.readUTF();
      var projects = AccessController.doPrivileged((PrivilegedAction<Map<String, ExprType>>) () ->
          (Map<String, ExprType>) new GsonBuilder().create().fromJson(projectsStr, Map.class));
      var projectList = projects.entrySet().stream()
          .map(e -> new NamedExpression(e.getKey(),
              new ReferenceExpression(e.getKey(), e.getValue())))
          .collect(Collectors.toList());
      var inputLoader = (PlanLoader) in.readObject();
      var input = (PhysicalPlan) inputLoader.apply(in, engine);
      return new ProjectOperator(input, projectList, List.of());
    };
    out.writeObject(loader);

    // Other types of Expressions are not supported and filtered out before
    var projects = projectList.stream().map(ne -> (ReferenceExpression)ne.getDelegated())
        .collect(Collectors.toMap(ReferenceExpression::getAttr, ReferenceExpression::type));

    // Being converted to json, this data can be compressed better than
    // list of NE or list of RE or name-type map
    out.writeUTF(
        AccessController.doPrivileged((PrivilegedAction<String>) () ->
            new GsonBuilder().create().toJson(projects)
    ));
    return input.getPlanForSerialization().writeExternal(out);
  }
}
