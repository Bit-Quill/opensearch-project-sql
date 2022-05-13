package org.opensearch.sql.sql.antlr;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.opensearch.sql.common.antlr.SyntaxCheckException;

/**
 * A base class that simplifies writing unit tests for SQLSyntaxparser.
 */
public class SQLParserTestBase {
  private final SQLSyntaxParser parser = new SQLSyntaxParser();

  /**
   * Use to check if query is accepted by the parser
   * @param query SQL query to test
   */
  protected void assertAccepted(String query) {
    assertNotNull(parser.parse(query), String.format("Failed to parse query: %s", query));
  }

  /**
   * Use to check if query is rejected by the parser.
   * @param query SQL query to test
   */
  protected void assertRejected(String query) {
    assertThrows(SyntaxCheckException.class, () -> parser.parse(query), String.format("Expected a SyntaxCheckException when parsing: %s", query));
  }
}
