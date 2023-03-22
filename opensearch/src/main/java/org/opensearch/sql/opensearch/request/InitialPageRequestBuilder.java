/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;
import static org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.DEFAULT_QUERY_TIMEOUT;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
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

  @Override
  public void pushDownFilter(QueryBuilder query) {
    QueryBuilder current = sourceBuilder.query();

    if (current == null) {
      sourceBuilder.query(query);
    } else {
      if (isBoolFilterQuery(current)) {
        ((BoolQueryBuilder) current).filter(query);
      } else {
        sourceBuilder.query(QueryBuilders.boolQuery()
            .filter(current)
            .filter(query));
      }
    }

    if (sourceBuilder.sorts() == null) {
      sourceBuilder.sort(DOC_FIELD_NAME, ASC); // Make sure consistent order
    }
  }

  /**
   * Push down sort to DSL request.
   *
   * @param sortBuilders sortBuilders.
   */
  @Override
  public void pushDownSort(List<SortBuilder<?>> sortBuilders) {
    if (isSortByDocOnly()) {
      sourceBuilder.sorts().clear();
    }

    for (SortBuilder<?> sortBuilder : sortBuilders) {
      sourceBuilder.sort(sortBuilder);
    }
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
  public void pushTypeMapping(Map<String, ExprType> typeMapping) {
    exprValueFactory.setTypeMapping(typeMapping);
  }

  private boolean isSortByDocOnly() {
    List<SortBuilder<?>> sorts = sourceBuilder.sorts();
    if (sorts != null) {
      return sorts.equals(Arrays.asList(SortBuilders.fieldSort(DOC_FIELD_NAME)));
    }
    return false;
  }
}
