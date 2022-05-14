package org.opensearch.sql.sql.antlr;

import org.junit.jupiter.api.Test;

public class RelevanceFunctionsParserTests extends SQLParserTestBase {

  @Test
  void acceptDoubleQuoteStar() {
    assertAccepted("SELECT * FROM T WHERE simple_query_string([\"*\"], \"sample\");");
  }
  @Test
  void acceptSingleQuoteStar() {
    assertAccepted("SELECT * FROM T WHERE simple_query_string(['*'], \"sample\");");
  }
  @Test
  void rejectMultipleStars() {
    assertRejected("SELECT * FROM T WHERE simple_query_string(['*', '*'], 'asdf');");
  }

  @Test
  void rejectUnquotedStar() {
    assertRejected("SELECT * FROM T WHERE simple_query_string([*], 'asdf');");
  }
  @Test
  void rejectAllFields() {
    assertRejected("SELECT * FROM T WHERE simple_query_string(*, 'asdf');");
  }

  @Test
  void rejectEmptyFieldList() {
    assertRejected("SELECT * FROM T WHERE simple_query_string([], 'asdf');");
  }

  @Test
  void rejectNoArguments() {
    assertRejected("SELECT * FROM T WHERE simple_query_string();");
  }
}
