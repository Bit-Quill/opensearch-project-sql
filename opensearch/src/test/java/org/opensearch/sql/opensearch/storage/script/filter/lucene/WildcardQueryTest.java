/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.expression.DSL.namedArgument;

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
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.WildcardQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class WildcardQueryTest {
  private final WildcardQuery wildcardQueryQuery = new WildcardQuery();
  private static final FunctionName wildcardQueryFunc = FunctionName.of("wildcard_query");

  static Stream<List<Expression>> generateValidData() {
    return Stream.of(
        List.of(
            DSL.namedArgument("field", DSL.literal("title")),
            DSL.namedArgument("query", DSL.literal("query_value*")),
            DSL.namedArgument("boost", DSL.literal("0.7")),
            DSL.namedArgument("case_insensitive", DSL.literal("false")),
            DSL.namedArgument("rewrite", DSL.literal("constant_score_boolean"))
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
    List<Expression> arguments = List.of(namedArgument("field", DSL.literal("title")));
    assertThrows(SyntaxCheckException.class,
        () -> wildcardQueryQuery.build(new WildcardQueryExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", DSL.literal("title")),
        namedArgument("query", DSL.literal("query_value*")),
        namedArgument("unsupported", DSL.literal("unsupported_value")));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> wildcardQueryQuery.build(new WildcardQueryExpression(arguments)));
  }

  @Test
  public void test_escaping_sql_wildcards() {
    assertEquals("%", wildcardQueryQuery.convertSqlWildcardToLucene("\\%"));
    assertEquals("\\*", wildcardQueryQuery.convertSqlWildcardToLucene("\\*"));
    assertEquals("_", wildcardQueryQuery.convertSqlWildcardToLucene("\\_"));
    assertEquals("\\?", wildcardQueryQuery.convertSqlWildcardToLucene("\\?"));
    assertEquals("%*", wildcardQueryQuery.convertSqlWildcardToLucene("\\%%"));
    assertEquals("*%", wildcardQueryQuery.convertSqlWildcardToLucene("%\\%"));
    assertEquals("%*%", wildcardQueryQuery.convertSqlWildcardToLucene("\\%%\\%"));
    assertEquals("*%*", wildcardQueryQuery.convertSqlWildcardToLucene("%\\%%"));
    assertEquals("_?", wildcardQueryQuery.convertSqlWildcardToLucene("\\__"));
    assertEquals("?_", wildcardQueryQuery.convertSqlWildcardToLucene("_\\_"));
    assertEquals("_?_", wildcardQueryQuery.convertSqlWildcardToLucene("\\__\\_"));
    assertEquals("?_?", wildcardQueryQuery.convertSqlWildcardToLucene("_\\__"));
    assertEquals("%\\*_\\?", wildcardQueryQuery.convertSqlWildcardToLucene("\\%\\*\\_\\?"));
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
