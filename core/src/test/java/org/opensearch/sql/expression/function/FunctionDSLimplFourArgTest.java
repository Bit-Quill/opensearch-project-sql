/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;

import java.util.List;

import static org.opensearch.sql.expression.function.FunctionDSL.impl;

class FunctionDSLimplFourArgTest extends FunctionDSLimplTestBase {

  @Override
  SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>
      getImplementationGenerator() {
    return impl(fourArgs, ANY_TYPE, ANY_TYPE, ANY_TYPE, ANY_TYPE, ANY_TYPE);
  }

  @Override
  List<Expression> getSampleArguments() {
    return List.of(DSL.literal(ANY), DSL.literal(ANY), DSL.literal(ANY), DSL.literal(ANY));
  }

  @Override
  String getExpected_toString() {
    return "sample(ANY, ANY, ANY, ANY)";
  }
}
