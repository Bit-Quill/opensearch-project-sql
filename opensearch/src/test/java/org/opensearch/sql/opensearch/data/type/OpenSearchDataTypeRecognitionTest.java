/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprBinaryValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprGeoPointValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprIpValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprTextKeywordValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprTextValue;

public class OpenSearchDataTypeRecognitionTest {

  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());

  @ParameterizedTest
  @MethodSource("types")
  public void typeof(String expected, ExprValue value) {
    assertEquals(expected, typeofGetValue(value));
  }

  private static Stream<Arguments> types() {
    // TODO: ARRAY and new types
    return Stream.of(
        Arguments.of("text", new OpenSearchExprTextValue("A")),
        Arguments.of("binary", new OpenSearchExprBinaryValue("A")),
        Arguments.of("ip", new OpenSearchExprIpValue("A")),
        Arguments.of("text", new OpenSearchExprTextKeywordValue("A")),
        Arguments.of("geo_point", new OpenSearchExprGeoPointValue(0d, 0d))
    );
  }

  private String typeofGetValue(ExprValue input) {
    return dsl.typeof(DSL.literal(input)).valueOf().stringValue();
  }
}
