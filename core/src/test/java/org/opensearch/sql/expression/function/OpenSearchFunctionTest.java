/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

public class OpenSearchFunctionTest extends ExpressionTestBase {
  private final NamedArgumentExpression field =
      new NamedArgumentExpression("field", DSL.literal("message"));
  private final NamedArgumentExpression query =
      new NamedArgumentExpression("query", DSL.literal("search query"));
  private final DataSourceMetadata defaultDataSourceMetadata = DataSourceMetadata.defaultOpenSearchDataSourceMetadata();

  @Test
  void test_opensearch_function() {
//    OpenSearchFunction function = mock(OpenSearchFunction.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    OpenSearchFunction function = new OpenSearchFunction(new FunctionName("match"), List.of(new NamedArgumentExpression("a", new LiteralExpression(new ExprStringValue("a")))));
    FunctionExpression expr = function;
//    assertEquals("match(field=\"message\", query=\"search query\")", expr.toString());
    assertEquals(BOOLEAN, function.type());
    assertThrows(UnsupportedOperationException.class,() -> function.valueOf(null));
    assertEquals("match(a=\"a\")", function.toString());
  }

//  @Test
//  void test_nested_function() {
////    OpenSearchFunction function = mock(OpenSearchFunction.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
//    OpenSearchFunction function = new OpenSearchFunction(new FunctionName("match"), List.of(new NamedArgumentExpression("a", new LiteralExpression(new ExprStringValue("a")))));
//    FunctionExpression expr = function;
////    assertEquals("match(field=\"message\", query=\"search query\")", expr.toString());
//    assertEquals(BOOLEAN, function.type());
//    assertThrows(UnsupportedOperationException.class,() -> function.valueOf(null));
//    assertEquals("match(a=\"a\")", function.toString());
//  }
}
