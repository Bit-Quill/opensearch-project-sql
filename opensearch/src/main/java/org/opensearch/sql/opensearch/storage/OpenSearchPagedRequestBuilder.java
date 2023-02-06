/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.DEFAULT_QUERY_TIMEOUT;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

public class OpenSearchPagedRequestBuilder {


  private final OpenSearchRequest.IndexName indexName;
  private final SearchSourceBuilder sourceBuilder;
  private final OpenSearchExprValueFactory exprValueFactory;
  private final int querySize;

  /**
   * Constructor.
   * @param indexName index being scanned
   * @param settings other settings
   * @param exprValueFactory value factory
   */
  public OpenSearchPagedRequestBuilder(OpenSearchRequest.IndexName indexName, Settings settings,
                                       OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.sourceBuilder = new SearchSourceBuilder();
    this.exprValueFactory = exprValueFactory;
    this.querySize = settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT);
    sourceBuilder.from(0);
    sourceBuilder.size(querySize);
    sourceBuilder.timeout(DEFAULT_QUERY_TIMEOUT);
  }

  public OpenSearchScrollRequest build() {
    return new OpenSearchScrollRequest(indexName, sourceBuilder, exprValueFactory);
  }

  public void pushDown(QueryBuilder query) {
    throw new RuntimeException();
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
    final Set<String> projectsSet =
        projects.stream().map(ReferenceExpression::getAttr).collect(Collectors.toSet());
    sourceBuilder.fetchSource(projectsSet.toArray(new String[0]), new String[0]);
  }

  public void pushTypeMapping(Map<String, ExprType> typeMapping) {
    exprValueFactory.setTypeMapping(typeMapping);
  }
}
