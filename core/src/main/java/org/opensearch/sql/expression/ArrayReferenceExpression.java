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
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;

@EqualsAndHashCode
public class ArrayReferenceExpression extends ReferenceExpression {
  @Getter
  private final OptionalInt index; // Should be a list of indexes to support multiple nesting levels
  @Getter
  private final ExprType type;
  @Getter
  private final List<String> paths;
  public ArrayReferenceExpression(String ref, ExprType type, int index) {
    super(ref, type);
    this.index = OptionalInt.of(index);
    this.type = type;
    this.paths = Arrays.asList(ref.split("\\."));
  }

  public ArrayReferenceExpression(String ref, ExprType type) {
    super(ref, type);
    this.index = OptionalInt.empty();
    this.type = type;
    this.paths = Arrays.asList(ref.split("\\."));
  }

  public ArrayReferenceExpression(ReferenceExpression ref) {
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
    return visitor.visitArrayReference(this, context);
  }

  public ExprValue resolve(ExprTupleValue value) {
    return resolve(value, paths);
  }

  private ExprValue resolve(ExprValue value, List<String> paths) {
    ExprValue wholePathValue = value.keyValue(String.join(PATH_SEP, paths));

    if (!index.isEmpty()) {
      wholePathValue = getIndexValueOfCollection(wholePathValue);
    } else if (paths.size() == 1 && !wholePathValue.type().equals(ExprCoreType.ARRAY)) {
      wholePathValue = ExprValueUtils.missingValue();
    }

    if (!wholePathValue.isMissing() || paths.size() == 1) {
      return wholePathValue;
    } else {
      return resolve(value.keyValue(paths.get(0)), paths.subList(1, paths.size()));
    }
  }

  private ExprValue getIndexValueOfCollection(ExprValue value) {
    ExprValue collectionVal = value;
    for (OptionalInt currentIndex : List.of(index)) {
      if (collectionVal.type().equals(ExprCoreType.ARRAY)) {
        collectionVal = collectionVal.collectionValue().get(currentIndex.getAsInt());
      } else {
        return ExprValueUtils.missingValue();
      }
    }
    return collectionVal;
  }
}
