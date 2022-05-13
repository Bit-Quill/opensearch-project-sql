package org.opensearch.sql.sql.antlr;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class RelevanceFunctionsParserTests extends SQLParserTestBase {

  @ParameterizedTest
  @MethodSource("simple_query_string_validQueries")
  public void testValidQuery(String query) {
    assertAccepted(query);
  }


  @ParameterizedTest
  @MethodSource("simple_query_string_invalidQueries")
  public void testInvalidQuery(String query) {
    assertRejected(query);
  }

  private static Stream<String> simple_query_string_invalidQueries() {
    return Stream.of(
        "SELECT * FROM T WHERE simple_query_string();",
        "SELECT * FROM T WHERE simple_query_string([], 'asdf');",
        "SELECT * FROM T WHERE simple_query_string(*, 'asdf');",
        "SELECT * FROM T WHERE simple_query_string([*], 'asdf');"
    );
  }

  private static Stream<String> simple_query_string_validQueries() {
    return Stream.of(
        "SELECT * FROM T WHERE simple_query_string(['*'], \"sample\");",
        "SELECT * FROM T WHERE simple_query_string([\"*\"], \"sample\");"
    );
  }
}
