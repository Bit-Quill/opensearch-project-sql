/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.script.aggregation.AggregationQueryBuilder;
import org.opensearch.sql.opensearch.storage.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;

/**
 * Index scan builder for aggregate query used by {@link OpenSearchIndexScanBuilder} internally.
 */
class OpenSearchIndexScanAggregationBuilder implements PushDownTranslator {

  /** OpenSearch index scan to be optimized. */
  private final OpenSearchRequestBuilder requestBuilder;

  /** Aggregators pushed down. */
  private List<NamedAggregator> aggregatorList;

  /** Grouping items pushed down. */
  private List<NamedExpression> groupByList;

  /** Sorting items pushed down. */
  private List<Pair<Sort.SortOption, Expression>> sortList;


  OpenSearchIndexScanAggregationBuilder(OpenSearchRequestBuilder requestBuilder) {
    this.requestBuilder = requestBuilder;
  }

  @Override
  public OpenSearchRequestBuilder build() {
    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(new DefaultExpressionSerializer());
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder =
        builder.buildAggregationBuilder(aggregatorList, groupByList, sortList);
    requestBuilder.pushDownAggregation(aggregationBuilder);
    requestBuilder.pushTypeMapping(
        builder.buildTypeMapping(aggregatorList, groupByList));
    return requestBuilder;
  }

  @Override
  public boolean pushDownAggregation(LogicalAggregation aggregation) {
    aggregatorList = aggregation.getAggregatorList();
    groupByList = aggregation.getGroupByList();
    return true;
  }

  @Override
  public boolean pushDownFilter(LogicalFilter filter) {
    return false;
  }

  @Override
  public boolean pushDownSort(LogicalSort sort) {
    if (hasAggregatorInSortBy(sort)) {
      return false;
    }

    sortList = sort.getSortList();
    return true;
  }

  @Override
  public boolean pushDownLimit(LogicalLimit limit) {
    return false;
  }

  @Override
  public boolean pushDownProject(LogicalProject project) {
    return false;
  }

  @Override
  public boolean pushDownHighlight(LogicalHighlight highlight) {
    return false;
  }

  @Override
  public boolean pushDownPageSize(LogicalPaginate paginate) {
    return false;
  }

  @Override
  public boolean pushDownNested(LogicalNested nested) {
    return false;
  }

  private boolean hasAggregatorInSortBy(LogicalSort sort) {
    final Set<String> aggregatorNames =
        aggregatorList.stream().map(NamedAggregator::getName).collect(Collectors.toSet());
    for (Pair<Sort.SortOption, Expression> sortPair : sort.getSortList()) {
      if (aggregatorNames.contains(((ReferenceExpression) sortPair.getRight()).getAttr())) {
        return true;
      }
    }
    return false;
  }
}
