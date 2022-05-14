package org.opensearch.sql.sql.antlr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

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
  @EnabledIfEnvironmentVariable(named = "WIP_TEST", matches = ".*")
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

  @Test
  @EnabledIfEnvironmentVariable(named = "WIP_TEST", matches = ".*")
  void rejectIntegerFieldName() {
    assertRejected("SELECT * FROM T WHERE simple_query_string([3], 'asdf');");
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "GITHUB_ISSUE", matches = "182")
  void rejectIntegerMatchFieldName() {
    assertRejected("SELECT * FROM T WHERE match(3, 'asdf')");
  }
}
