/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;

@RequiredArgsConstructor
public class SubsequentPageRequestBuilder implements PagedRequestBuilder {
  final OpenSearchRequest.IndexName indexName;
  final String scrollId;
  final OpenSearchExprValueFactory exprValueFactory;

  @Override
  public OpenSearchRequest build() {
    return new ContinueScrollRequest(scrollId, exprValueFactory);
  }

  @Override
  public OpenSearchRequest.IndexName getIndexName() {
    return indexName;
  }
}
