/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import lombok.EqualsAndHashCode;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Builder for a paged opensearch request.
 * Override pushDown* methods from TableScaneBuilder as more features
 * support pagination.
 */
public class OpenSearchPagedScanBuilder extends TableScanBuilder {
  @EqualsAndHashCode.Include
  OpenSearchPagedIndexScan indexScan;

  public OpenSearchPagedScanBuilder(OpenSearchPagedIndexScan indexScan) {
    this.indexScan = indexScan;
  }


  @Override
  public TableScanOperator build() {
    return indexScan;
  }
}
