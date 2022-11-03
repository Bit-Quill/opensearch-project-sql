/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprValue;
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
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.WildcardQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class WildcardQueryTest {
  private static final DSL dsl = new ExpressionConfig()
      .dsl(new ExpressionConfig().functionRepository());
  private final WildcardQuery wildcardQueryQuery = new WildcardQuery();
  private final FunctionName wildcardQueryFunc = FunctionName.of("wildcard_query");

  static Stream<List<Expression>> generateValidData() {
    return Stream.of(
        List.of(
            dsl.namedArgument("field", "title"),
            dsl.namedArgument("query", "query_value*")
        )
    );
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters(List<Expression> validArgs) {
    Assertions.assertNotNull(wildcardQueryQuery.build(
        new WildcardQueryExpression(validArgs)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments() {
    List<Expression> arguments = List.of();
    assertThrows(SyntaxCheckException.class,
        () -> wildcardQueryQuery.build(new WildcardQueryExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument() {
    List<Expression> arguments = List.of(namedArgument("field", "title"));
    assertThrows(SyntaxCheckException.class,
        () -> wildcardQueryQuery.build(new WildcardQueryExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "title"),
        namedArgument("query", "query_value*"),
        namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> wildcardQueryQuery.build(new WildcardQueryExpression(arguments)));
  }

  private NamedArgumentExpression namedArgument(String name, String value) {
    return dsl.namedArgument(name, DSL.literal(value));
  }

  private NamedArgumentExpression namedArgument(String name, LiteralExpression value) {
    return dsl.namedArgument(name, value);
  }

  private class WildcardQueryExpression extends FunctionExpression {
    public WildcardQueryExpression(List<Expression> arguments) {
      super(WildcardQueryTest.this.wildcardQueryFunc, arguments);
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
