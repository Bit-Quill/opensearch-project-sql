/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression;

import static org.opensearch.sql.utils.ExpressionUtils.PATH_SEP;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;

@EqualsAndHashCode
public class IndexedReferenceExpression extends ReferenceExpression {
  @Getter
  private final OptionalInt index;
  @Getter
  private final ExprType type;
  @Getter
  private final List<String> paths;
  public IndexedReferenceExpression(String ref, ExprType type, int index) {
    super(ref, type);
    this.index = OptionalInt.of(index);
    this.type = type;
    this.paths = Arrays.asList(ref.split("\\."));
  }

  public IndexedReferenceExpression(ReferenceExpression ref) {
    super(ref.toString(), ref.type());
    this.index = OptionalInt.empty();
    this.type = ref.type();
    this.paths = Arrays.asList(ref.toString().split("\\."));
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> env) {
    return env.resolve(this);
  }

  @Override
  public ExprType type() {
    return type;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitIndexedReference(this, context);
  }

  public ExprValue resolve(ExprTupleValue value) {
    return resolve(value, paths);
  }

  private ExprValue resolve(ExprValue value, List<String> paths) {
    ExprValue wholePathValue = value.keyValue(String.join(PATH_SEP, paths));
    // For array types only first index currently supported.
    if (!index.isEmpty() && wholePathValue.type().equals(ExprCoreType.ARRAY)) {
      wholePathValue = wholePathValue.collectionValue().get(index.getAsInt());
    }

    if (!wholePathValue.isMissing() || paths.size() == 1) {
      return wholePathValue;
    } else {
      return resolve(value.keyValue(paths.get(0)), paths.subList(1, paths.size()));
    }
  }
}
