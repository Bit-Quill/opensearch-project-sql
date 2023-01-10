/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.WideningTypeRule;
import org.opensearch.sql.exception.ExpressionEvaluationException;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class VarargsFunctionResolverTest {
  @Mock
  private FunctionSignature exactlyMatchFS;
  @Mock
  private FunctionSignature bestMatchFS;
  @Mock
  private FunctionSignature leastMatchFS;
  @Mock
  private FunctionSignature notMatchFS;
  @Mock
  private FunctionSignature functionSignature;
  @Mock
  private FunctionBuilder exactlyMatchBuilder;
  @Mock
  private FunctionBuilder bestMatchBuilder;
  @Mock
  private FunctionBuilder leastMatchBuilder;
  @Mock
  private FunctionBuilder notMatchBuilder;

  private FunctionName functionName = FunctionName.of("test_function");

  @Test
  void resolve_function_signature_exactly_match() {
    when(functionSignature.match(exactlyMatchFS)).thenReturn(WideningTypeRule.TYPE_EQUAL);
    when(functionSignature.getParamTypeList()).thenReturn(ImmutableList.of(STRING, STRING, STRING));
    VarargsFunctionResolver resolver = new VarargsFunctionResolver(functionName,
        ImmutableMap.of(exactlyMatchFS, exactlyMatchBuilder));

    assertEquals(exactlyMatchBuilder, resolver.resolve(functionSignature).getValue());
  }

  @Test
  void resolve_function_signature_best_match() {
    when(functionSignature.match(bestMatchFS)).thenReturn(1);
    when(functionSignature.match(leastMatchFS)).thenReturn(2);
    when(functionSignature.getParamTypeList()).thenReturn(ImmutableList.of(STRING, STRING, STRING));
    VarargsFunctionResolver resolver = new VarargsFunctionResolver(functionName,
        ImmutableMap.of(bestMatchFS, bestMatchBuilder, leastMatchFS, leastMatchBuilder));

    assertEquals(bestMatchBuilder, resolver.resolve(functionSignature).getValue());
  }

  @Test
  void resolve_function_not_match() {
    when(functionSignature.match(notMatchFS)).thenReturn(WideningTypeRule.IMPOSSIBLE_WIDENING);
    // accepts 2 or more arguments, but passing one
    when(functionSignature.getParamTypeList()).thenReturn(ImmutableList.of(STRING));
    VarargsFunctionResolver resolver = new VarargsFunctionResolver(functionName,
        ImmutableMap.of(notMatchFS, notMatchBuilder));

    assertThrows(ExpressionEvaluationException.class, () -> resolver.resolve(functionSignature));
  }
}
