/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.DEFAULT_QUERY_TIMEOUT;

import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

/**
 * This builder assists creating the initial OpenSearch paging (scrolling) request.
 * It is used only on the first page (pagination request).
 * Subsequent requests (cursor requests) use {@link ContinuePageRequestBuilder}.
 */
public class InitialPageRequestBuilder extends PagedRequestBuilder {

  @Getter
  private final OpenSearchRequest.IndexName indexName;
  private final SearchSourceBuilder sourceBuilder;
  private final OpenSearchExprValueFactory exprValueFactory;

  /**
   * Constructor.
   * @param indexName index being scanned
   * @param pageSize page size
   * @param exprValueFactory value factory
   */
  // TODO accept indexName as string (same way as `OpenSearchRequestBuilder` does)?
  public InitialPageRequestBuilder(OpenSearchRequest.IndexName indexName,
                                   int pageSize,
                                   OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.exprValueFactory = exprValueFactory;
    this.sourceBuilder = new SearchSourceBuilder()
        .from(0)
        .size(pageSize)
        .timeout(DEFAULT_QUERY_TIMEOUT);
  }

  @Override
  public OpenSearchScrollRequest build() {
    return new OpenSearchScrollRequest(indexName, sourceBuilder, exprValueFactory);
  }

  /**
   * Push down project expression to OpenSearch.
   */
  @Override
  public void pushDownProjects(Set<ReferenceExpression> projects) {
    sourceBuilder.fetchSource(projects.stream().map(ReferenceExpression::getAttr)
        .distinct().toArray(String[]::new), new String[0]);
  }

  @Override
  public void pushTypeMapping(Map<String, OpenSearchDataType> typeMapping) {
    exprValueFactory.extendTypeMapping(typeMapping);
  }
}
