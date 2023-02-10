/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.storage.TableScanOperator;

@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class OpenSearchPagedIndexScan extends TableScanOperator {
  private final OpenSearchClient client;
  private final PagedRequestBuilder requestBuilder;
  @EqualsAndHashCode.Include
  @ToString.Include
  private OpenSearchRequest request;
  private Iterator<ExprValue> iterator;

  public OpenSearchPagedIndexScan(OpenSearchClient client,
                                  PagedRequestBuilder requestBuilder) {
    this.client = client;
    this.requestBuilder = requestBuilder;
  }

  @Override
  public String explain() {
    throw new RuntimeException("Implement OpenSearchPagedIndexScan.explain");
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
  public void open() {
    super.open();
    request = requestBuilder.build();
    OpenSearchResponse response = client.search(request);
    if (!response.isEmpty()) {
      iterator = response.iterator();
    } else {
      iterator = Collections.emptyIterator();
    }
  }

  @Override
  public void close() {
    super.close();

    client.cleanup(request);
  }

  @Override
  public String toCursor() {
    // TODO this assumes exactly one index is scanned.
    var indexName = requestBuilder.getIndexName().getIndexNames()[0];
    var cursor = request.toCursor();
    return cursor == null || cursor.isEmpty()
        ? "" : createSection("OpenSearchPagedIndexScan", indexName, cursor);
  }
}
