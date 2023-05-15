package org.opensearch.sql.sql.parser;

public class AstBuilderAnonymizer extends AstBuilder {
  public AstBuilderAnonymizer(String query) {
    super(new AstExpressionBuilderAnonymizer(), query);
  }


}