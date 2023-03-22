/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder;
import org.opensearch.sql.opensearch.storage.script.sort.SortQueryBuilder;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanQueryBuilder.findReferenceExpressions;

/**
 * Builder for a paged OpenSearch request.
 * Override pushDown* methods from TableScanBuilder as more features
 * support pagination.
 */
public class OpenSearchPagedIndexScanBuilder extends TableScanBuilder {
  @EqualsAndHashCode.Include
  OpenSearchPagedIndexScan indexScan;

  public OpenSearchPagedIndexScanBuilder(OpenSearchPagedIndexScan indexScan) {
    this.indexScan = indexScan;
  }

  @Override
  public boolean pushDownFilter(LogicalFilter filter) {
    FilterQueryBuilder queryBuilder = new FilterQueryBuilder(
        new DefaultExpressionSerializer());
    QueryBuilder query = queryBuilder.build(filter.getCondition());
    indexScan.getRequestBuilder().pushDownFilter(query);
    return true;
  }

  @Override
  public boolean pushDownSort(LogicalSort sort) {
    List<Pair<Sort.SortOption, Expression>> sortList = sort.getSortList();
    final SortQueryBuilder builder = new SortQueryBuilder();
    indexScan.getRequestBuilder().pushDownSort(sortList.stream()
        .map(sortItem -> builder.build(sortItem.getValue(), sortItem.getKey()))
        .collect(Collectors.toList()));
    return true;
  }

  // How can we set limit? Regular request sets
  //   .size(limit)
  // Paged request sets
  //   .size(pageSize)
  @Override
  public boolean pushDownLimit(LogicalLimit limit) {
    return false;//super.pushDownLimit(limit);
  }

  @Override
  public boolean pushDownProject(LogicalProject project) {
    indexScan.getRequestBuilder().pushDownProjects(
        findReferenceExpressions(project.getProjectList()));
    return false;
  }

  @Override
  public TableScanOperator build() {
    return indexScan;
  }
}
