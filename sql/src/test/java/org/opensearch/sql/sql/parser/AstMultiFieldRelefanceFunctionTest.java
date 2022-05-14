package org.opensearch.sql.sql.parser;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.common.antlr.SyntaxCheckException;

public class AstMultiFieldRelefanceFunctionTest extends AstExpressionBuilderTestBase {
  @Test
  @EnabledIfEnvironmentVariable(named = "WIP_TEST", matches = ".*")
  public void testAllFieldsQuery() {
    assertAstEquals(AstDSL.function("simple_query_string", AstDSL.allFieldsList(),
            AstDSL.unresolvedArg("query", AstDSL.stringLiteral("asdf"))),
        "SIMPLE_QUERY_STRING(['*'], 'asdf')");
  }

  @Test
  public void rejectFieldSpec() {
    assertThrows(SyntaxCheckException.class,
        () -> buildExprAst("SIMPLE_QUERY_STRING(['test'], 'asdf')"),
        "Only * field specification is supported right now");
  }
}
