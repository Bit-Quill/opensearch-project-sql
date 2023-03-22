/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import lombok.Getter;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

public abstract class PagedRequestBuilder extends PushDownRequestBuilder {

  @Getter
  protected OpenSearchExprValueFactory exprValueFactory;

  abstract public OpenSearchRequest build();

  abstract public OpenSearchRequest.IndexName getIndexName();
}
