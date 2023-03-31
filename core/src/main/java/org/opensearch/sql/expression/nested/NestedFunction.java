/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.nested;

import java.util.List;
import java.util.stream.Collectors;

import lombok.Builder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.OpenSearchFunctions;

public class NestedFunction extends OpenSearchFunctions.OpenSearchFunction {
  private final List<Expression> arguments;

  /**
   * Required argument constructor.
   * @param functionName name of the function
   * @param arguments a list of expressions
   */
  public NestedFunction(FunctionName functionName, List<Expression> arguments) {
    super(functionName, arguments);
    this.arguments = arguments;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    return valueEnv.resolve(this.arguments.get(0));
  }

  @Override
  public ExprType type() {
    return  this.arguments.get(0).type();
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", getFunctionName(),
        arguments.stream()
            .map(Object::toString)
            .collect(Collectors.joining(", ")));
  }
}
