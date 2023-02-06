package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.COMPACT;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParser;
import java.util.Arrays;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.protocol.response.QueryResult;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class JsonTypeResponseFormatterTest {

  private final JsonTypeResponseFormatter formatter = new JsonTypeResponseFormatter(COMPACT);

  @Test
  void format_response() {
    QueryResult response = new QueryResult(
        new ExecutionEngine.Schema(ImmutableList.of(
            new ExecutionEngine.Schema.Column("name", "name", STRING),
            new ExecutionEngine.Schema.Column("address1", "address1", OPENSEARCH_TEXT),
            new ExecutionEngine.Schema.Column("address2", "address2", OPENSEARCH_TEXT_KEYWORD),
            new ExecutionEngine.Schema.Column("location", "location", STRUCT),
            new ExecutionEngine.Schema.Column("employer", "employer", ARRAY),
            new ExecutionEngine.Schema.Column("age", "age", INTEGER))),
            ImmutableList.of(
                    tupleValue(ImmutableMap.<String, Object>builder()
                            .put("name", "John")
                            .put("address1", "Seattle")
                            .put("address2", "WA")
                            .put("location", ImmutableMap.of("x", "1", "y", "2"))
                            .put("employments", ImmutableList.of(
                                    ImmutableMap.of("name", "Amazon"),
                                    ImmutableMap.of("name", "AWS")))
                            .put("age", 20)
                            .build())));

    assertJsonEquals(
            "{\n"
                    + "   \"_shards\":{\n"
                    + "      \"total\":0,\n"
                    + "      \"successful\":0,\n"
                    + "      \"skipped\":0,\n"
                    + "      \"failed\":0\n" + "   },\n"
                    + "   \"hits\":{\n"
                    + "      \"total\":{\n"
                    + "         \"value\":1,\n"
                    + "         \"relation\":\"eq\"\n"
                    + "      },\n"
                    + "      \"hits\":[\n"
                    + "         {\n"
                    + "            \"_source\":{\n"
                    + "               \"address2\":\"WA\",\n"
                    + "               \"address1\":\"Seattle\",\n"
                    + "               \"name\":\"John\",\n"
                    + "               \"employer\":[\n"
                    + "                  {\n"
                    + "                     \"name\":\"Amazon\"\n"
                    + "                  },\n"
                    + "                  {\n"
                    + "                     \"name\":\"AWS\"\n"
                    + "                  }\n"
                    + "               ],\n"
                    + "               \"location\":{\n"
                    + "                  \"x\":\"1\",\n"
                    + "                  \"y\":\"2\"\n"
                    + "               },\n"
                    + "               \"age\":20\n"
                    + "            }\n"
                    + "         }\n"
                    + "      ]\n"
                    + "   }\n"
                    + "}",
            formatter.format(response));
  }

  @Test
  void format_response_with_missing_and_null_value() {
    QueryResult response =
        new QueryResult(
            new ExecutionEngine.Schema(ImmutableList.of(
                  new ExecutionEngine.Schema.Column("name", null, STRING),
                  new ExecutionEngine.Schema.Column("age", null, INTEGER))),
          Arrays.asList(
                  ExprTupleValue.fromExprValueMap(
                          ImmutableMap.of("name", stringValue("John"), "age", LITERAL_MISSING)),
                  ExprTupleValue.fromExprValueMap(
                          ImmutableMap.of("name", stringValue("Allen"), "age", LITERAL_NULL)),
                  tupleValue(ImmutableMap.of("name", "Smith", "age", 30))));

    assertEquals(
        "{\"_shards\":{\"total\":0,\"successful\":0,\"skipped\":0,\"failed\":0},"
                + "\"hits\":{\"total\":{\"value\":3,\"relation\":\"eq\"},"
                + "\"hits\":[{\"_source\":{\"name\":\"John\"}},{\"_source\":{\"name\":\"Allen\"}},"
                + "{\"_source\":{\"name\":\"Smith\",\"age\":30}}]}}",
            formatter.format(response));
  }

  @Test
  void format_client_error_response_due_to_syntax_exception() {
    assertJsonEquals(
        "{\"error\":"
                + "{\""
                + "type\":\"SyntaxCheckException\","
                + "\"reason\":\"Invalid Query\","
                + "\"details\":\"Invalid query syntax\""
                + "},"
                + "\"status\":400}",
            formatter.format(new SyntaxCheckException("Invalid query syntax"))
    );
  }

  @Test
  void format_client_error_response_due_to_semantic_exception() {
    assertJsonEquals(
        "{\"error\":"
                + "{\""
                + "type\":\"SemanticCheckException\","
                + "\"reason\":\"Invalid Query\","
                + "\"details\":\"Invalid query semantics\""
                + "},"
                + "\"status\":400}",
            formatter.format(new SemanticCheckException("Invalid query semantics"))
    );
  }

  @Test
  void format_server_error_response() {
    assertJsonEquals(
        "{\"error\":"
                + "{\""
                + "type\":\"IllegalStateException\","
                + "\"reason\":\"There was internal problem at backend\","
                + "\"details\":\"Execution error\""
                + "},"
                + "\"status\":503}",
            formatter.format(new IllegalStateException("Execution error"))
    );
  }

  @Test
  void format_server_error_response_due_to_opensearch() {
    assertJsonEquals(
        "{\"error\":"
                + "{\""
                + "type\":\"OpenSearchException\","
                + "\"reason\":\"Error occurred in OpenSearch engine: all shards failed\","
                + "\"details\":\"OpenSearchException[all shards failed]; "
                + "nested: IllegalStateException[Execution error];; "
                + "java.lang.IllegalStateException: Execution error\\n"
                + "For more details, please send request for Json format to see the raw response "
                + "from OpenSearch engine.\""
                + "},"
                + "\"status\":503}",
            formatter.format(new OpenSearchException("all shards failed",
                    new IllegalStateException("Execution error")))
    );
  }

  private static void assertJsonEquals(String expected, String actual) {
    assertEquals(
            JsonParser.parseString(expected),
            JsonParser.parseString(actual));
  }

}

