/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.protocol.response.format;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.AGGREGATION_FUNC_MAPPING;
import static org.opensearch.sql.protocol.response.format.ErrorFormatter.compactJsonifyWithNullValues;
import static org.opensearch.sql.protocol.response.format.ErrorFormatter.prettyJsonifyWithNulls;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import com.google.gson.annotations.SerializedName;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import org.opensearch.sql.protocol.response.QueryResult;

public class JsonJsonResponseFormatter extends JsonResponseFormatter<QueryResult> {

  private final Style style;

  public JsonJsonResponseFormatter(Style style) {
    super(style);
    this.style = style;
  }

  @Override
  public Object buildJsonObject(QueryResult response) {
    JsonResponse.JsonResponseBuilder json = JsonResponse.builder();
    Total total = new Total(response.size(), "eq");
    Shards shards = new Shards(1, 1, 0, 0);
    List<String> columnNames = new LinkedList<>(response.columnNameTypes().keySet());
    List<Hit> hitList = new LinkedList<>();
    Map<String, Aggregation> aggregations = new LinkedHashMap<>();

    int id = 1;
    for (Object[] values : getRows(response)) {
      List<Object> valueList = new LinkedList<>(Arrays.asList(values));
      Map<String, Object> source = combineListsIntoMap(columnNames, valueList);

      Map<String, Object> filteredSource = filterSourceFromFunctions(source);
      Map<String, Object> functions = getFunctions(source);
      Map<String, Aggregation> aggregationMap = getAggregations(source);
      aggregations.putAll(aggregationMap);

      Hit hit = new Hit(response.getIndexName(), Integer.toString(id), 1.0, filteredSource,
              functions.isEmpty() ? null : functions);

      hitList.add(hit);
      id++;
    }
    Hits hits = new Hits(total, 1.0, hitList, aggregations.isEmpty() ? null : aggregations);

    json.took(0)
            .timedOut(false)
            .shards(shards)
            .hits(hits);

    return json.build();
  }

  @Override
  protected String jsonify(Object jsonObject) {
    return AccessController.doPrivileged((PrivilegedAction<String>) () ->
            (style == PRETTY) ? prettyJsonifyWithNulls(jsonObject)
                    : compactJsonifyWithNullValues(jsonObject));
  }

  private Map<String, Aggregation> getAggregations(Map<String, Object> source) {
    return source.entrySet()
            .stream().filter(entry -> isAggregationFunction(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey,
                    stringObjectEntry -> new Aggregation(stringObjectEntry.getValue())));
  }

  private Map<String, Object> getFunctions(Map<String, Object> source) {
    return source.entrySet()
            .stream().filter(entry -> isFunctionName(entry.getKey())
                    && !isAggregationFunction(entry.getKey()))
            .collect(HashMap::new, (map, values) ->
                    map.put(values.getKey(), values.getValue()), HashMap::putAll);
  }

  private Map<String, Object> filterSourceFromFunctions(Map<String, Object> source) {
    return source.entrySet()
            .stream().filter(entry -> !isFunctionName(entry.getKey()))
            .collect(HashMap::new, (map, values) ->
                    map.put(values.getKey(), values.getValue()), HashMap::putAll);
  }

  private Map<String, Object> combineListsIntoMap(List<String> keys, List<Object> values) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      map.put(keys.get(i), values.get(i));
    }
    return map;
  }

  private List<Object[]> getRows(QueryResult response) {
    List<Object[]> rows = new LinkedList<>();
    for (Object[] values : response) {
      rows.add(values);
    }
    return rows;
  }

  private boolean isAggregationFunction(String value) {
    return isFunctionName(value)
            && AGGREGATION_FUNC_MAPPING.containsKey(value.split("[(]")[0].toLowerCase());
  }

  private boolean isFunctionName(String value) {
    return value.contains("(") && value.contains(")");
  }

  @Builder
  @Getter
  public static class JsonResponse {
    public final int took;
    @SerializedName("timed_out")
    public final boolean timedOut;
    @SerializedName("_shards")
    public final Shards shards;
    public final Hits hits;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Hits {
    public final Total total;
    @SerializedName("max_score")
    public final double maxScore;
    @Singular("column")
    public final List<Hit> hits;
    public final Map<String, Aggregation> aggregations;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Hit {
    @SerializedName("_index")
    public final String index;
    @SerializedName("_id")
    public final String id;
    @SerializedName("_score")
    public final double score;
    @SerializedName("_source")
    public final Map<String, Object> source;
    public final Map<String, Object> fields;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Shards {
    public final int total;
    public final int successful;
    public final int skipped;
    public final int failed;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Total {
    public final int value;
    public final String relation;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Aggregation {
    public final Object value;
  }


}
