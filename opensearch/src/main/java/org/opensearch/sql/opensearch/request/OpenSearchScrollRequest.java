/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

/**
 * OpenSearch scroll search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation 2)
 * Maintain scroll ID between calls to client search method
 */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchScrollRequest implements OpenSearchRequest {
  private SearchRequest initialSearchRequest;
  /** Scroll context timeout. */
  private TimeValue scrollTimeout;

  /**
   * {@link OpenSearchRequest.IndexName}.
   */
  private IndexName indexName;

  /** Index name. */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private OpenSearchExprValueFactory exprValueFactory;
  /**
   * Scroll id which is set after first request issued. Because ElasticsearchClient is shared by
   * multi-thread so this state has to be maintained here.
   */
  @Setter
  @Getter
  private String scrollId;

  private boolean needClean = false;

  private List<String> includes;

  /** Default constructor for Externalizable only.
   */
  public OpenSearchScrollRequest() {
  }

  /** Constructor. */
  public OpenSearchScrollRequest(IndexName indexName,
                                 TimeValue scrollTimeout,
                                 SearchSourceBuilder sourceBuilder,
                                 OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.scrollTimeout = scrollTimeout;
    this.exprValueFactory = exprValueFactory;
    this.initialSearchRequest = new SearchRequest()
        .indices(indexName.getIndexNames())
        .scroll(scrollTimeout)
        .source(sourceBuilder);

    includes = sourceBuilder.fetchSource() != null && sourceBuilder.fetchSource().includes() != null
      ? Arrays.asList(sourceBuilder.fetchSource().includes())
      : List.of();
    }


  /** Constructor. */
  @Override
  public OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    SearchResponse openSearchResponse;
    if (isScroll()) {
      openSearchResponse = scrollAction.apply(scrollRequest());
    } else {
      openSearchResponse = searchAction.apply(initialSearchRequest);
    }

    var response = new OpenSearchResponse(openSearchResponse, exprValueFactory, includes);
    needClean = response.isEmpty();
    if (!needClean) {
      setScrollId(openSearchResponse.getScrollId());
    }
    return response;
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    try {
      // clean on the last page only, to prevent closing the scroll/cursor in the middle of paging.
      if (needClean && isScroll()) {
        cleanAction.accept(getScrollId());
        setScrollId(null);
      }
    } finally {
      reset();
    }
  }

  /**
   * Is scroll started which means pages after first is being requested.
   *
   * @return true if scroll started
   */
  public boolean isScroll() {
    return scrollId != null;
  }

  /**
   * Generate OpenSearch scroll request by scroll id maintained.
   *
   * @return scroll request
   */
  public SearchScrollRequest scrollRequest() {
    Objects.requireNonNull(scrollId, "Scroll id cannot be null");
    return new SearchScrollRequest().scroll(scrollTimeout).scrollId(scrollId);
  }

  /**
   * Reset internal state in case any stale data. However, ideally the same instance is not supposed
   * to be reused across different physical plan.
   */
  public void reset() {
    scrollId = null;
  }

  /**
   * Convert a scroll request to string that can be included in a cursor.
   * @return a string representing the scroll request.
   */
  @Override
  public boolean hasAnotherBatch() {
    return !needClean && scrollId != null && !scrollId.equals("");
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    initialSearchRequest.writeTo(out);
    out.writeTimeValue(scrollTimeout);
    if (!needClean) {
      // If needClean is true, there is no more data to get from OpenSearch and scrollId is
      // used only to clean up OpenSearch context.
      out.writeString(scrollId);
    }
    out.writeBoolean(needClean);
    out.writeStringCollection(includes);
    indexName.writeTo(out);
  }

  public OpenSearchScrollRequest(StreamInput in, OpenSearchStorageEngine engine) throws IOException {
    initialSearchRequest = new SearchRequest(in);
    scrollTimeout = in.readTimeValue();
    scrollId = in.readString();
    needClean = in.readBoolean();
    includes = in.readStringList();
    indexName = new IndexName(in);
    exprValueFactory = new OpenSearchExprValueFactory(((OpenSearchIndex) engine.getTable(null, indexName.toString())).getFieldOpenSearchTypes());
  }
}
