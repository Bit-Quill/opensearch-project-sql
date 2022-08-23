/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.expression.env.Environment.extendEnv;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;

/**
 * HighlightOperator class.
 */
@EqualsAndHashCode
public class HighlightOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final Expression highlight;

  public HighlightOperator(PhysicalPlan input, Expression highlight) {
    this.input = input;
    this.highlight = highlight;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitHighlight(this, context);
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public ExprValue next() {
    ExprValue inputValue = input.next();
    Pair<String, ExprValue> evalMap = mapHighlight(inputValue.bindingTuples());

    if (STRUCT == inputValue.type()) {
      ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
      Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);
      for (Map.Entry<String, ExprValue> valueEntry : tupleValue.entrySet()) {
        resultBuilder.put(valueEntry);
      }
      resultBuilder.put(evalMap);
      return ExprTupleValue.fromExprValueMap(resultBuilder.build());
    } else {
      return inputValue;
    }
  }

  /**
   * Evaluate the expression in the {@link HighlightOperator#highlight} with {@link Environment}.
   * @param env {@link Environment}
   * @return The mapping of reference and {@link ExprValue} for expression.
   */
  private Pair<String, ExprValue> mapHighlight(Environment<Expression, ExprValue> env) {
    String osHighlightKey = "_highlight." + StringUtils.unquoteText(highlight.toString());
    ReferenceExpression osOutputVar = DSL.ref(osHighlightKey, STRING);

    String sqlHighlightKey = "highlight(" + highlight.toString() + ")";
    ReferenceExpression sqlOutputVar = DSL.ref(sqlHighlightKey, STRING);

    // Add mapping for sql output and opensearch returned highlight fields
    ExprValue value = osOutputVar.valueOf(env);
    extendEnv(env, sqlOutputVar, value);

    return new ImmutablePair<>(sqlOutputVar.toString(), value);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return List.of(this.input);
  }
}
