/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.ContinuePageRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchPagedIndexScan;
import org.opensearch.sql.opensearch.storage.system.OpenSearchSystemIndex;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.TableScanOperator;

/** OpenSearch storage engine implementation. */
@RequiredArgsConstructor
public class OpenSearchStorageEngine implements StorageEngine {

  /** OpenSearch client connection. */
  private final OpenSearchClient client;

  private final Settings settings;

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String name) {
    if (isSystemIndex(name)) {
      return new OpenSearchSystemIndex(client, name);
    } else {
      return new OpenSearchIndex(client, settings, name);
    }
  }

  @Override
  public TableScanOperator getTableScan(String indexName, String scrollId) {
    // TODO call `getTable` here?
    var index = new OpenSearchIndex(client, settings, indexName);
    var requestBuilder = new ContinuePageRequestBuilder(
        new OpenSearchRequest.IndexName(indexName),
        scrollId,
        new OpenSearchExprValueFactory(index.getFieldTypes()));
    return new OpenSearchPagedIndexScan(client, requestBuilder);
  }
}
