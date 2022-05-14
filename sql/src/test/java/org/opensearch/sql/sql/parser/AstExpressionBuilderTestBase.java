package org.opensearch.sql.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.antlr.v4.runtime.CommonTokenStream;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;

public class AstExpressionBuilderTestBase {
  private final AstExpressionBuilder astExprBuilder = new AstExpressionBuilder();

  protected Node buildExprAst(String expr) {
    OpenSearchSQLLexer lexer = new OpenSearchSQLLexer(new CaseInsensitiveCharStream(expr));
    OpenSearchSQLParser parser = new OpenSearchSQLParser(new CommonTokenStream(lexer));
    parser.addErrorListener(new SyntaxAnalysisErrorListener());
    return parser.expression().accept(astExprBuilder);
  }

  protected void assertAstEquals(Node expectedAst, String expression) {
    assertEquals(expectedAst, buildExprAst(expression));
  }
}
