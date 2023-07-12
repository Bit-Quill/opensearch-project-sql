/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression;

import static org.opensearch.sql.utils.ExpressionUtils.PATH_SEP;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;

@EqualsAndHashCode
public class ArrayReferenceExpression extends ReferenceExpression {
  @Getter
  private final List<Pair<String, OptionalInt>> partsAndIndexes;
  @Getter
  private final ExprType type;
  public ArrayReferenceExpression(String ref, ExprType type, List<Pair<String, OptionalInt>> partsAndIndexes) {
    super(StringUtils.removeParenthesis(ref), type);
    this.partsAndIndexes = partsAndIndexes;
    this.type = type;
  }

  public ArrayReferenceExpression(ReferenceExpression ref) {
    super(StringUtils.removeParenthesis(ref.toString()), ref.type());
    this.partsAndIndexes = Arrays.stream(ref.toString().split("\\.")).map(e -> Pair.of(e, OptionalInt.empty())).collect(
        Collectors.toList());
    this.type = ref.type();
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
    return resolve(value, partsAndIndexes);
  }

  private ExprValue resolve(ExprValue value, List<Pair<String, OptionalInt>> paths) {
    List<String> pathsWithoutParenthesis =
        paths.stream().map(p -> StringUtils.removeParenthesis(p.getLeft())).collect(Collectors.toList());
    ExprValue wholePathValue = value.keyValue(String.join(PATH_SEP, pathsWithoutParenthesis));

    if (!paths.get(0).getRight().isEmpty()) {
      if (value.keyValue(pathsWithoutParenthesis.get(0)) instanceof ExprCollectionValue) { // TODO check array size
        wholePathValue = value
            .keyValue(pathsWithoutParenthesis.get(0))
            .collectionValue()
            .get(paths.get(0).getRight().getAsInt());
        if (paths.size() != 1) {
          return resolve(wholePathValue, paths.subList(1, paths.size()));
        }
      } else {
        return ExprValueUtils.missingValue();
      }
    } else if (wholePathValue.isMissing()) {
      return resolve(value.keyValue(pathsWithoutParenthesis.get(0)), paths.subList(1, paths.size()));
    }

    if (!wholePathValue.isMissing() || paths.size() == 1) {
      return wholePathValue;
    } else {
      return resolve(value.keyValue(pathsWithoutParenthesis.get(0)), paths.subList(1, paths.size()));
    }
  }
}
