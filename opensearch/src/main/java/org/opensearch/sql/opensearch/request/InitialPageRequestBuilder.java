/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.DEFAULT_QUERY_TIMEOUT;

import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

public class InitialPageRequestBuilder extends PagedRequestBuilder {

  @Getter
  private final OpenSearchRequest.IndexName indexName;
  private final SearchSourceBuilder sourceBuilder;

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

  public void pushDown(QueryBuilder query) {
    throw new UnsupportedOperationException("pushdown of a query is not supported");
  }

  public void pushDownAggregation(
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder) {

    throw new UnsupportedOperationException("pagination of aggregation requests is not supported");
  }

  public void pushDownSort(List<SortBuilder<?>> sortBuilders) {
    throw new UnsupportedOperationException("sorting of paged requests is not supported");

  }

  public void pushDownLimit(Integer limit, Integer offset) {
    throw new UnsupportedOperationException("limit of paged requests is not supported");
  }

  public void pushDownHighlight(String field, Map<String, Literal> arguments) {
    throw new UnsupportedOperationException("highlight of paged requests is not supported");
  }

  /**
   * Push down project expression to OpenSearch.
   */
  public void pushDownProjects(Set<ReferenceExpression> projects) {
    sourceBuilder.fetchSource(projects.stream().map(ReferenceExpression::getAttr)
        .distinct().toArray(String[]::new), new String[0]);
  }

  public void pushTypeMapping(Map<String, ExprType> typeMapping) {
    exprValueFactory.setTypeMapping(typeMapping);
  }
}
