/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.nested;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionDSL;
import java.util.List;

@UtilityClass
public class NestedFunctions {
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(nested());
  }

  private static DefaultFunctionResolver nested() {
    return FunctionDSL.define(BuiltinFunctionName.NESTED.getName(),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), BYTE, BYTE),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), SHORT, SHORT),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), INTEGER, INTEGER),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), LONG, LONG),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), FLOAT, FLOAT),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), DOUBLE, DOUBLE),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), STRING, STRING),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), BOOLEAN, BOOLEAN),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), DATE, DATE),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), TIME, TIME),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), DATETIME, DATETIME),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), TIMESTAMP, TIMESTAMP),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), INTERVAL, INTERVAL),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), STRUCT, STRUCT),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), ARRAY, ARRAY),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_single_param), STRING, ARRAY),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_double_param), STRING, STRING, ARRAY),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_double_param), BOOLEAN, ARRAY, BOOLEAN));
  }

  private ExprValue nested_single_param(ExprValue field) {
    return field;
  }

  private ExprValue nested_double_param(ExprValue field1, ExprValue field2) {
    return new ExprCollectionValue(List.of(field1, field2));
  }
}
