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

import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionDSL;

@UtilityClass
public class NestedFunctions {
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(nested());
  }

  private static DefaultFunctionResolver nested() {
    return FunctionDSL.define(BuiltinFunctionName.NESTED.getName(),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                (v1) -> v1
            ), STRING, STRING),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                (v1) -> v1
            ), INTEGER, INTEGER),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                (v1) -> v1
            ), LONG, LONG),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                (v1) -> v1
            ), FLOAT, FLOAT),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                (v1) -> v1
            ), DOUBLE, DOUBLE),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                (v1) -> v1
            ), BOOLEAN, BOOLEAN),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                (v1) -> v1
            ), STRUCT, STRUCT),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                (v1) -> v1
            ), ARRAY, ARRAY),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, STRING, STRING, ARRAY),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, INTEGER, INTEGER, ARRAY),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, LONG, LONG, ARRAY),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, FLOAT, FLOAT, ARRAY),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, DOUBLE, DOUBLE, ARRAY),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, BOOLEAN, BOOLEAN, ARRAY),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, STRUCT, STRUCT, ARRAY),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, ARRAY, ARRAY, ARRAY),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, STRING, STRING, STRUCT),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, INTEGER, INTEGER, STRUCT),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, LONG, LONG, STRUCT),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, FLOAT, FLOAT, STRUCT),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, DOUBLE, DOUBLE, STRUCT),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, BOOLEAN, BOOLEAN, STRUCT),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, STRUCT, STRUCT, STRUCT),
        FunctionDSL.impl(
            (v1, v2) -> {
              return nullMissingFirstArgOnlyHandling(v1);
            }, ARRAY, ARRAY, STRUCT));
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
