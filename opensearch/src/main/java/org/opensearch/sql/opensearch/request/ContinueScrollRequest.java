/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.sql.opensearch.request.OpenSearchScrollRequest.DEFAULT_SCROLL_TIMEOUT;

import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

@EqualsAndHashCode
public class ContinueScrollRequest implements OpenSearchRequest {
  final String initialScrollId;

  // ScrollId that OpenSearch returns after search.
  String responseScrollId;

  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  @Getter
  private final OpenSearchExprValueFactory exprValueFactory;

  public ContinueScrollRequest(String scrollId, OpenSearchExprValueFactory exprValueFactory) {
    this.initialScrollId = scrollId;
    this.exprValueFactory = exprValueFactory;
  }

  @Override
  public OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    SearchResponse openSearchResponse = scrollAction.apply(new SearchScrollRequest(initialScrollId)
        .scroll(DEFAULT_SCROLL_TIMEOUT));

    // TODO if terminated_early - something went wrong, e.g. no scroll returned.
    var response = new OpenSearchResponse(openSearchResponse, exprValueFactory);
    if (!response.isEmpty()) {
      responseScrollId = openSearchResponse.getScrollId();
    } // else - last empty page, we should ignore the scroll even if it is returned
    return response;
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    cleanAction.accept(responseScrollId);
  }

  @Override
  public SearchSourceBuilder getSourceBuilder() {
    throw new UnsupportedOperationException(
        "SearchSourceBuilder is unavailable for ContinueScrollRequest");
  }

  @Override
  public String toCursor() {
    return responseScrollId;
  }
}
