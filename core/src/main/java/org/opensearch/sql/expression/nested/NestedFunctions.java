/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.nested;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionDSL;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

@UtilityClass
public class NestedFunctions {
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(nested());
  }

  private static DefaultFunctionResolver nested() {
    return FunctionDSL.define(BuiltinFunctionName.NESTED.getName(),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_str), STRING, STRUCT, STRING));
  }

  private ExprValue nested_returns_str(ExprValue obj, ExprValue field) {
    return obj.tupleValue().get(field.stringValue());
  }
}
