/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.DEFAULT_QUERY_TIMEOUT;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
public class InitialPageRequestBuilderTest {

  @Mock
  private OpenSearchExprValueFactory exprValueFactory;

  private final int pageSize = 42;

  private final OpenSearchRequest.IndexName indexName = new OpenSearchRequest.IndexName("test");

  private InitialPageRequestBuilder requestBuilder;

  @BeforeEach
  void setup() {
    requestBuilder = new InitialPageRequestBuilder(
        indexName, pageSize, exprValueFactory);
  }

  @Test
  public void build() {
    assertEquals(
        new OpenSearchScrollRequest(indexName,
            new SearchSourceBuilder()
                .from(0)
                .size(pageSize)
                .timeout(DEFAULT_QUERY_TIMEOUT),
            exprValueFactory),
        requestBuilder.build()
    );
  }

  @Test
  public void pushDown_not_supported() {
    assertAll(
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownFilter(mock())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownAggregation(mock())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownSort(mock())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownLimit(1, 2)),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownHighlight("", Map.of()))
    );
  }

  @Test
  public void pushTypeMapping() {
    Map<String, ExprType> typeMapping = Map.of("intA", INTEGER);
    requestBuilder.pushTypeMapping(typeMapping);

    verify(exprValueFactory).setTypeMapping(typeMapping);
  }

  @Test
  public void pushDownProject() {
    Set<ReferenceExpression> references = Set.of(DSL.ref("intA", INTEGER));
    requestBuilder.pushDownProjects(references);

    assertEquals(
        new OpenSearchScrollRequest(indexName,
            new SearchSourceBuilder()
                .from(0)
                .size(pageSize)
                .timeout(DEFAULT_QUERY_TIMEOUT)
                .fetchSource(new String[]{"intA"}, new String[0]),
            exprValueFactory),
        requestBuilder.build()
    );
  }

  @Test
  public void getIndexName() {
    assertEquals(indexName, requestBuilder.getIndexName());
  }
}
