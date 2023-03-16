/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

public interface PagedRequestBuilder {
  OpenSearchRequest build();

  OpenSearchRequest.IndexName getIndexName();
}
