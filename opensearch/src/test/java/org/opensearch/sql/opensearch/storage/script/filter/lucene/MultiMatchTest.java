/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MultiMatchQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class MultiMatchTest {
  private static final DSL dsl = new ExpressionConfig()
      .dsl(new ExpressionConfig().functionRepository());
  private final MultiMatchQuery multiMatchQuery = new MultiMatchQuery();
  private final FunctionName multiMatchName = FunctionName.of("multimatch");
  private final FunctionName multi_matchName = FunctionName.of("multi_match");
  private final FunctionName multiMatchQueryName = FunctionName.of("multimatchquery");
  private final FunctionName[] functionNames =
      {multiMatchName, multi_matchName, multiMatchQueryName};
  private static final LiteralExpression fields_value = DSL.literal(
      new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
          "title", ExprValueUtils.floatValue(1.F),
          "body", ExprValueUtils.floatValue(.3F)))));
  private static final LiteralExpression query_value = DSL.literal("query_value");

  static Stream<List<Expression>> generateValidData() {
    return Stream.of(
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value)
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("analyzer", DSL.literal("simple"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("auto_generate_synonyms_phrase_query", DSL.literal("true"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("boost", DSL.literal("1.3"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("cutoff_frequency", DSL.literal("4.2"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("fuzziness", DSL.literal("AUTO:2,4"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("fuzzy_transpositions", DSL.literal("true"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("lenient", DSL.literal("true"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("max_expansions", DSL.literal("7"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("minimum_should_match", DSL.literal("4"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("operator", DSL.literal("AND"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("prefix_length", DSL.literal("7"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("tie_breaker", DSL.literal("0.3"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("type", DSL.literal("cross_fields"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("zero_terms_query", DSL.literal("ALL"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("zero_terms_query", DSL.literal("all"))
        )
      );
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters(List<Expression> validArgs) {
    for (FunctionName funcName: functionNames) {
      Assertions.assertNotNull(multiMatchQuery.build(
          new MultiMatchExpression(validArgs, funcName)));
    }
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments() {
    List<Expression> arguments = List.of();
    for (FunctionName funcName: functionNames) {
      assertThrows(SyntaxCheckException.class,
          () -> multiMatchQuery.build(new MultiMatchExpression(arguments, funcName)));
    }
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument() {
    List<Expression> arguments = List.of(namedArgument("fields", fields_value));
    for (FunctionName funcName: functionNames) {
      assertThrows(SyntaxCheckException.class,
          () -> multiMatchQuery.build(new MultiMatchExpression(arguments, funcName)));
    }
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("fields", fields_value),
        namedArgument("query", query_value),
        dsl.namedArgument("unsupported", "unsupported_value"));
    for (FunctionName funcName: functionNames) {
      Assertions.assertThrows(SemanticCheckException.class,
          () -> multiMatchQuery.build(new MultiMatchExpression(arguments, funcName)));
    }
  }

  private NamedArgumentExpression namedArgument(String name, LiteralExpression value) {
    return dsl.namedArgument(name, value);
  }

  private class MultiMatchExpression extends FunctionExpression {
    public MultiMatchExpression(List<Expression> arguments, FunctionName funcName) {
      super(funcName, arguments);
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      throw new UnsupportedOperationException("Invalid function call, "
          + "valueOf function need implementation only to support Expression interface");
    }

    @Override
    public ExprType type() {
      throw new UnsupportedOperationException("Invalid function call, "
          + "type function need implementation only to support Expression interface");
    }
  }
}
