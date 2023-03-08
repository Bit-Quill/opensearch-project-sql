/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.nested;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionDSL;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
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
            FunctionDSL.nullMissingHandling(
                (v1) -> v1
            ), LONG, LONG),//TODO we are going to need to do this for all single params, double params types i think
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                (v1) -> v1
            ), STRING, STRING),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                (v1, v2) -> v1
            ), STRING, STRING, ARRAY),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                (v1, v2) -> v1
            ), STRING, STRING, STRUCT)); // TODO test this with data
  }
}
