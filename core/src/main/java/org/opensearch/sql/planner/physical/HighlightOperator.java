/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.HighlightExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.expression.env.Environment.extendEnv;

/**
 * Common method actions for ml-commons related operators.
 */

@Getter
@EqualsAndHashCode
public class HighlightOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final Expression highlight;
  @EqualsAndHashCode.Exclude
  private ExprValue next;
  private static final Predicate<ExprValue> NULL_OR_MISSING = v -> v.isNull() || v.isMissing();

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
//    while (input.hasNext()) {
//      ExprValue next = input.next();
//
//      String refName = "_highlight" + "." + StringUtils.unquoteText(highlight.toString());
//      ExprValue hl = next.bindingTuples().resolve(DSL.ref(refName, ExprCoreType.STRING));
//
////      ExprValue hl = highlight.valueOf(next.bindingTuples());
//      if (!(hl.isNull() || hl.isMissing())) {
//        this.next = next;
//        return true;
//      }
//    }
//    return false;

    return input.hasNext();
  }

  @Override
  public ExprValue next() {
//    Environment<Expression, ExprValue> val = this.next.bindingTuples();
//    String refName = "_highlight" + "." + StringUtils.unquoteText(highlight.get(0).toString());
//    return val.resolve(DSL.ref(refName, ExprCoreType.STRING));

//    return this.next;

    ExprValue inputValue = input.next();
    Map<String, ExprValue> evalMap = eval(inputValue.bindingTuples());

    if (STRUCT == inputValue.type()) {
      ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
      Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);
      for (Map.Entry<String, ExprValue> valueEntry : tupleValue.entrySet()) {
        if (evalMap.containsKey(valueEntry.getKey())) {
          resultBuilder.put(valueEntry.getKey(), evalMap.get(valueEntry.getKey()));
          evalMap.remove(valueEntry.getKey());
        } else {
          resultBuilder.put(valueEntry);
        }
      }
      resultBuilder.putAll(evalMap);
      return ExprTupleValue.fromExprValueMap(resultBuilder.build());
    } else {
      return inputValue;
    }
  }

  /**
   * Evaluate the expression in the {@link EvalOperator#expressionList} with {@link Environment}.
   * @param env {@link Environment}
   * @return The mapping of reference and {@link ExprValue} for each expression.
   */
  private Map<String, ExprValue> eval(Environment<Expression, ExprValue> env) {
    Map<String, ExprValue> evalResultMap = new LinkedHashMap<>();
//    for (Pair<ReferenceExpression, Expression> pair : expressionList) {
      HighlightExpression var = new HighlightExpression(highlight);
      ExprValue value = highlight.valueOf(env);
      env = extendEnv(env, var, value);
      evalResultMap.put(var.toString(), value);
//    }
    return evalResultMap;
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return List.of(this.input);
  }
}
