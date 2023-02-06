/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;

public class SubsequentPageRequestBuilder implements PagedRequestBuilder {
  private OpenSearchRequest.IndexName indexName;
  final String scrollId;
  private OpenSearchExprValueFactory exprValueFactory;

  public SubsequentPageRequestBuilder(OpenSearchRequest.IndexName indexName, String scanAsString,
                                      OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    scrollId = scanAsString;
    this.exprValueFactory = exprValueFactory;
  }

  @Override
  public OpenSearchRequest build() {
    return new ContinueScrollRequest(scrollId, exprValueFactory);
  }

  @Override
  public OpenSearchRequest.IndexName getIndexName() {
    return indexName;
  }
}
