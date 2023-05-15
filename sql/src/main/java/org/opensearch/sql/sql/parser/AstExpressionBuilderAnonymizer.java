package org.opensearch.sql.sql.parser;

import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;

public class AstExpressionBuilderAnonymizer extends AstExpressionBuilder {
  @Override
  public UnresolvedExpression visitColumnName(OpenSearchSQLParser.ColumnNameContext ctx) {
    return new QualifiedName("TEST");
  }
}