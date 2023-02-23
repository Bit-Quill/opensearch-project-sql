/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

@RequiredArgsConstructor
public class SubsequentPageRequestBuilder implements PagedRequestBuilder {

  @Getter
  private final OpenSearchRequest.IndexName indexName;
  private final String scrollId;
  private final OpenSearchExprValueFactory exprValueFactory;

  @Override
  public OpenSearchRequest build() {
    return new ContinueScrollRequest(scrollId, exprValueFactory);
  }
}
