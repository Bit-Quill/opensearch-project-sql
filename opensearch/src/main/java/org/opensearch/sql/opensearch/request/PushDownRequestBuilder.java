/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

public abstract class PushDownRequestBuilder {
  protected boolean isBoolFilterQuery(QueryBuilder current) {
    return (current instanceof BoolQueryBuilder);
  }

  private void throwUnsupported(String operation) {
    throw new UnsupportedOperationException(String.format("%s: %s in requests is not supported",
        getClass().getSimpleName(), operation));
  }

  public void pushDownFilter(QueryBuilder query) {
    throwUnsupported("filter");
  }

  public void pushDownAggregation(
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder) {
    throwUnsupported("aggregation");
  }

  public void pushDownSort(List<SortBuilder<?>> sortBuilders) {
    throwUnsupported("sorting");
  }

  public void pushDownLimit(Integer limit, Integer offset) {
    throwUnsupported("limit/offset");
  }

  public void pushDownHighlight(String field, Map<String, Literal> arguments) {
    throwUnsupported("highlight");
  }

  public void pushDownProjects(Set<ReferenceExpression> projects) {
    throwUnsupported("select list");
  }

  public void pushTypeMapping(Map<String, ExprType> typeMapping) {
    throwUnsupported("type mapping");
  }
}
