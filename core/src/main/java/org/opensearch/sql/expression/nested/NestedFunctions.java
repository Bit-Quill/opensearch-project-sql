/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.nested;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import java.util.ArrayList;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.SerializableFunction;

@UtilityClass
public class NestedFunctions {
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(nested());
  }

  List<Pair<ExprCoreType, ExprCoreType>> singleParamFunctionTypes =
      List.of(
          Pair.of(STRING, STRING),
          Pair.of(INTEGER, INTEGER),
          Pair.of(LONG, LONG),
          Pair.of(FLOAT, FLOAT),
          Pair.of(DOUBLE, DOUBLE),
          Pair.of(BOOLEAN, BOOLEAN),
          Pair.of(STRUCT, STRUCT),
          Pair.of(ARRAY, ARRAY)
      );

  List<Triple<ExprCoreType, ExprCoreType, ExprCoreType>> doubleParamFunctionTypes =
      List.of(
          Triple.of(STRING, STRING, STRING),
          Triple.of(STRING, STRING, ARRAY),
          Triple.of(INTEGER, INTEGER, ARRAY),
          Triple.of(LONG, LONG, ARRAY),
          Triple.of(FLOAT, FLOAT, ARRAY),
          Triple.of(DOUBLE, DOUBLE, ARRAY),
          Triple.of(BOOLEAN, BOOLEAN, ARRAY),
          Triple.of(STRUCT, STRUCT, ARRAY),
          Triple.of(ARRAY, ARRAY, ARRAY),
          Triple.of(STRING, STRING, STRUCT),
          Triple.of(INTEGER, INTEGER, STRUCT),
          Triple.of(LONG, LONG, STRUCT),
          Triple.of(FLOAT, FLOAT, STRUCT),
          Triple.of(DOUBLE, DOUBLE, STRUCT),
          Triple.of(BOOLEAN, BOOLEAN, STRUCT),
          Triple.of(STRUCT, STRUCT, STRUCT),
          Triple.of(ARRAY, ARRAY, STRUCT)
      );

  private static DefaultFunctionResolver nested() {
    List<SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>> functions =
        new ArrayList<>();
    singleParamFunctionTypes.forEach(
        singleParam -> functions.add(
            impl(
              nullMissingHandling(
                  (v1) -> v1
              ),
                singleParam.getLeft(), singleParam.getRight()
            )
        )
    );

    doubleParamFunctionTypes.forEach(
        doubleParam -> functions.add(
            impl(
                (v1, v2) -> {
                  return nullMissingFirstArgOnlyHandling(v1);
                },
                doubleParam.getLeft(), doubleParam.getMiddle(), doubleParam.getRight()
            )
        )
    );

    return define(BuiltinFunctionName.NESTED.getName(),
        functions
    );
  }

  /**
   * Wrapper the binary ExprValue function with default
   * NULL and MISSING handling for first arg only.
   */
  public static ExprValue nullMissingFirstArgOnlyHandling(ExprValue v1) {
    if (v1.isMissing()) {
      return ExprValueUtils.missingValue();
    } else if (v1.isNull()) {
      return ExprValueUtils.nullValue();
    } else {
      return v1;
    }
  }
}
