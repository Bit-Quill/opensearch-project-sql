/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.system;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.opensearch.request.system.OpenSearchSystemRequest;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * OpenSearch index scan operator.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class OpenSearchSystemIndexScan extends TableScanOperator {
  /**
   * OpenSearch client.
   */
  private final OpenSearchSystemRequest request;

  /**
   * Search response for current batch.
   */
  private Iterator<ExprValue> iterator;

  private long totalHits = 0;

  private List<ExprValue> rawResponse = List.of();

  @Override
  public void open() {
    rawResponse = request.search();
    totalHits = rawResponse.size();
    iterator = rawResponse.iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public long getTotalHits() {
    return totalHits;
  }

  @Override
  public String getRawResponse() {
    return rawResponse.stream().map(ExprValueUtils::jsonify)
        .collect(Collectors.joining(", ", "[ ", " ]"));
  }

  @Override
  public String explain() {
    return request.toString();
  }
}
