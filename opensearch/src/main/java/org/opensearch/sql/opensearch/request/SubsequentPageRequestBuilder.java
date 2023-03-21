/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import lombok.Getter;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

public class SubsequentPageRequestBuilder extends PagedRequestBuilder {

  public SubsequentPageRequestBuilder(OpenSearchRequest.IndexName indexName,
                                      String scrollId,
                                      OpenSearchExprValueFactory factory) {
    this.indexName = indexName;
    this.scrollId = scrollId;
    this.exprValueFactory = factory;
  }

  @Getter
  private final OpenSearchRequest.IndexName indexName;
  private final String scrollId;

  @Override
  public OpenSearchRequest build() {
    return new ContinueScrollRequest(scrollId, exprValueFactory);
  }
}
