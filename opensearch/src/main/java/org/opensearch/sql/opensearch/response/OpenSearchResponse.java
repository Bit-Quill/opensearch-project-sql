/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.response;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

/**
 * OpenSearch search response.
 */
@EqualsAndHashCode
@ToString
public class OpenSearchResponse implements Iterable<ExprValue> {

  /**
   * Search query result (non-aggregation).
   */
  private final SearchHits hits;

  /**
   * Search aggregation result.
   */
  private final Aggregations aggregations;

  /**
   * ElasticsearchExprValueFactory used to build ExprValue from search result.
   */
  @EqualsAndHashCode.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /**
   * Constructor of ElasticsearchResponse.
   */
  public OpenSearchResponse(SearchResponse searchResponse,
                            OpenSearchExprValueFactory exprValueFactory) {
    this.hits = searchResponse.getHits();
    this.aggregations = searchResponse.getAggregations();
    this.exprValueFactory = exprValueFactory;
  }

  /**
   * Constructor of ElasticsearchResponse with SearchHits.
   */
  public OpenSearchResponse(SearchHits hits, OpenSearchExprValueFactory exprValueFactory) {
    this.hits = hits;
    this.aggregations = null;
    this.exprValueFactory = exprValueFactory;
  }

  /**
   * Is response empty. As OpenSearch doc says, "Each call to the scroll API returns the next batch
   * of results until there are no more results left to return, ie the hits array is empty."
   *
   * @return true for empty
   */
  public boolean isEmpty() {
    return (hits.getHits() == null) || (hits.getHits().length == 0) && aggregations == null;
  }

  public boolean isAggregationResponse() {
    return aggregations != null;
  }

  /**
   * Make response iterable without need to return internal data structure explicitly.
   *
   * @return search hit iterator
   */
  public Iterator<ExprValue> iterator() {
    if (isAggregationResponse()) {
      return exprValueFactory.getParser().parse(aggregations).stream().map(entry -> {
        ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
        for (Map.Entry<String, Object> value : entry.entrySet()) {
          builder.put(value.getKey(), exprValueFactory.construct(value.getKey(), value.getValue()));
        }
        return (ExprValue) ExprTupleValue.fromExprValueMap(builder.build());
      }).iterator();
    } else {
      return Arrays.stream(hits.getHits())
          .map(hit -> {
            ExprValue docData = exprValueFactory.construct(hit.getSourceAsString());
            Map<String, Object> rowSource = hit.getSourceAsMap();
            List<String> head = docData.tupleValue().keySet().stream().collect(Collectors.toList());
            rowSource = flatRow(head, rowSource);
            docData = ExprValueUtils.tupleValue(rowSource);
            if (hit.getHighlightFields().isEmpty()) {
              return docData;
            } else {
              ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
              builder.putAll(docData.tupleValue());
              var hlBuilder = ImmutableMap.<String, ExprValue>builder();
              for (var es : hit.getHighlightFields().entrySet()) {
                hlBuilder.put(es.getKey(), ExprValueUtils.collectionValue(
                    Arrays.stream(es.getValue().fragments()).map(
                        t -> (t.toString())).collect(Collectors.toList())));
              }
              builder.put("_highlight", ExprTupleValue.fromExprValueMap(hlBuilder.build()));
              return ExprTupleValue.fromExprValueMap(builder.build());
            }
          }).iterator();
    }
  }

  /**
   * Simplifies the structure of row's source Map by flattening it, making the full path of an object the key
   * and the Object it refers to the value. This handles the case of regular object since nested objects will not
   * be in hit.source but rather in hit.innerHits
   * <p>
   * Sample input:
   * keys = ['comments.likes']
   * row = comments: {
   * likes: 2
   * }
   * <p>
   * Return:
   * flattenedRow = {comment.likes: 2}
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> flatRow(List<String> keys, Map<String, Object> row) {
    Map<String, Object> flattenedRow = new HashMap<>();
    for (String key : keys) {
      String[] splitKeys = key.split("\\.");
      boolean found = true;
      Object currentObj = row;

      for (String splitKey : splitKeys) {
        // This check is made to prevent Cast Exception as an ArrayList of objects can be in the sourceMap
        if (!(currentObj instanceof Map)) {
          found = false;
          break;
        }

        Map<String, Object> currentMap = (Map<String, Object>) currentObj;
        if (!currentMap.containsKey(splitKey)) {
          found = false;
          break;
        }

        currentObj = currentMap.get(splitKey);
      }

      if (found) {
        flattenedRow.put(key, currentObj);
      }
    }

    return flattenedRow;
  }
}
