/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage.system;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.prometheus.request.system.PrometheusSystemRequest;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * Prometheus table scan operator.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class PrometheusSystemTableScan extends TableScanOperator {

  @EqualsAndHashCode.Include
  private final PrometheusSystemRequest request;

  private Iterator<ExprValue> iterator;

  private List<ExprValue> rawResponse = List.of();

  @Override
  public void open() {
    rawResponse = request.search();
    iterator = rawResponse.iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public String getRawResponse() {
    return rawResponse.stream().map(ExprValueUtils::jsonify)
        .collect(Collectors.joining(", ", "[ ", " ]"));
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public String explain() {
    return request.toString();
  }
}
