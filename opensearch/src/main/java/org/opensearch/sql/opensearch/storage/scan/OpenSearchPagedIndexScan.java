/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.PaginatedPlanCache;
import org.opensearch.sql.expression.function.SerializableBiFunction;
import org.opensearch.sql.expression.function.SerializableFunction;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.ContinueScrollRequest;
import org.opensearch.sql.opensearch.request.InitialPageRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.PagedRequestBuilder;
import org.opensearch.sql.opensearch.request.SubsequentPageRequestBuilder;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.TableScanOperator;

@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class OpenSearchPagedIndexScan extends TableScanOperator {
  private OpenSearchClient client;
  private PagedRequestBuilder requestBuilder;
  @EqualsAndHashCode.Include
  @ToString.Include
  private OpenSearchRequest request;
  private Iterator<ExprValue> iterator;
  private long totalHits = 0;

  public OpenSearchPagedIndexScan() {
    int a = 5;
    // TODO validate that called only from deserializer
  }

  public OpenSearchPagedIndexScan(OpenSearchClient client,
                                  PagedRequestBuilder requestBuilder) {
    this.client = client;
    this.requestBuilder = requestBuilder;
  }

  @Override
  public String explain() {
    throw new NotImplementedException("Implement OpenSearchPagedIndexScan.explain");
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
      totalHits = response.getTotalHits();
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
  public long getTotalHits() {
    return totalHits;
  }

//  @Override
//  public void prepareToSerialization(PaginatedPlanCache.SerializationContext context) {
//    context.setIndexName(requestBuilder.getIndexName().toString());
//    context.setScrollId(request.toCursor());
//  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    //out.writeObject(requestBuilder.getExprValueFactory());

    PlanLoader loader = (in, engine) -> {
      var indexName = (String) in.readUTF();
      var scrollId = (String) in.readUTF();
      return engine.getTableScan(indexName, scrollId);
    };
    out.writeObject(loader);
    out.writeUTF(requestBuilder.getIndexName().toString());
    out.writeUTF(request.toCursor());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // nothing, everything done by loader
  }
/*
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    //var factory = (OpenSearchExprValueFactory) in.readObject();
    var indexName = (String) in.readUTF();
    var scrollId = (String) in.readUTF();
    requestBuilder = new SubsequentPageRequestBuilder(new OpenSearchRequest.IndexName(indexName),
        //scrollId, factory);
        scrollId, new OpenSearchExprValueFactory(Map.of()));

    ModulesBuilder modules = new ModulesBuilder();
    var injector = modules.createInjector();
    client = SecurityAccess.doPrivileged(() -> injector.getInstance(OpenSearchClient.class));
  }*/
}
