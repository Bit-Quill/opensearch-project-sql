/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import org.json.JSONObject;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.SimpleQueryStringQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SimpleQueryStringQueryTest {
  DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());

  @Test
  void required_parameters_default_rest_call_test() {
    var builder = new SimpleQueryStringQuery();
    var ast = dsl.simple_query_string(dsl.namedArgument("fields", DSL.literal("*")),
        dsl.namedArgument("query", DSL.literal("test")));
    String restCall = builder.build(ast).toString();
    assertJsonEquals("{\n"
        + "   \"simple_query_string\" : {\n"
        + "   \"fields\": [\n"
        + "       \"*^1.0\"\n"
        + "   ],\n"
        + "   \"query\" : \"test\",\n"
        + "   \"flags\" : -1,\n"
        + "   \"default_operator\" : \"or\",\n"
        + "   \"analyze_wildcard\" : false,\n"
        + "   \"auto_generate_synonyms_phrase_query\" : true,\n"
        + "   \"fuzzy_prefix_length\" : 0,\n"
        + "   \"fuzzy_max_expansions\" : 50,\n"
        + "   \"fuzzy_transpositions\" : true,\n"
        + "   \"boost\" : 1.0\n"
        + "  }\n"
        + "}",
        restCall);


  }


  private static void assertJsonEquals(String expected, String actual) {
    assertTrue(new JSONObject(expected).similar(new JSONObject(actual)),
        StringUtils.format("Expected: %s, actual: %s", expected, actual));
  }
}