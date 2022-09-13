/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.expression.env.Environment.extendEnv;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;

/**
 * HighlightOperator evaluates the  {@link HighlightOperator#highlight} to put result
 * into the output. Highlight fields in input are matched to the appropriate output.
 * Direct mapping between input and output, as well as partial mapping is made
 * dependent on highlight expression.
 *
 */
@EqualsAndHashCode
@AllArgsConstructor
public class HighlightOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final Expression highlight;
  @Getter
  private final Map<String, Literal> arguments;
  @Getter
  private final String name;

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
    String osHighlightKey = "_highlight";
    String highlightFieldStr = StringUtils.unquoteText(highlight.toString());
    if (!highlightFieldStr.contains("*")) {
      osHighlightKey += "." + highlightFieldStr;
    }

    ReferenceExpression osOutputVar = DSL.ref(osHighlightKey, STRING);
    ExprValue value = osOutputVar.valueOf(env);

    // In the event of multiple returned highlights and wildcard being
    // used in conjunction with other highlight calls, we need to ensure
    // only wildcard regex matching is mapped to wildcard call.
    if (highlightFieldStr.contains("*") && value.type() == STRUCT) {
      value = new ExprTupleValue(
          new LinkedHashMap<String, ExprValue>(value.tupleValue()
              .entrySet()
              .stream()
              .filter(s -> matchesHighlightRegex(s.getKey(), highlightFieldStr))
              .collect(Collectors.toMap(
                  e -> e.getKey(),
                  e -> e.getValue()))));
    }

    ReferenceExpression sqlOutputVar = DSL.ref(name, STRING);

    // Add mapping for sql output and opensearch returned highlight fields
    extendEnv(env, sqlOutputVar, value);

    return new ImmutablePair<>(sqlOutputVar.toString(), value);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return List.of(this.input);
  }

  /**
   * Check if field matches the wildcard pattern used in highlight query.
   * @param field Highlight selected field for query
   * @param pattern Wildcard regex to match field against
   * @return True if field matches wildcard pattern
   */
  private boolean matchesHighlightRegex(String field, String pattern) {
    Pattern p = Pattern.compile(pattern.replace("*", ".*"));
    Matcher matcher = p.matcher(field);
    return matcher.matches();
  }
}
