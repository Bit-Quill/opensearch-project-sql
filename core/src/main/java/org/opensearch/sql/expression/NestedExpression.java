/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Getter;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * Nested Expression.
 */
@Getter
public class NestedExpression extends FunctionExpression {

  private final Expression field;
  private final Expression nestedField;
  private final ExprType type;

  /**
   * NestedExpression Constructor.
   * @param nestedField : Nested field for expression.
   */
  public NestedExpression(Expression field, Expression nestedField) {
    super(BuiltinFunctionName.NESTED.getName(), List.of(field, nestedField));
    this.field = field;
    this.nestedField = nestedField;
    this.type = ExprCoreType.STRING;
  }

  /**
   * Return collection value matching nested field.
   * @param valueEnv : Dataset to parse value from.
   * @return : collection value of nested fields.
   */
  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    String refName = "_nested" + "." + StringUtils.unquoteText(getNestedField().toString());
    ExprValue value = valueEnv.resolve(DSL.ref(refName, ExprCoreType.STRING));
    return value;
  }

  /**
   * Get type for NestedExpression.
   * @return : Expression type.
   */
  @Override
  public ExprType type() {
    return this.type;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitNested(this, context);
  }
}