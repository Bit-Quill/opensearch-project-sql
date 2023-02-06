/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.protocol.response.format;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.AGGREGATION_FUNC_MAPPING;

import com.google.gson.annotations.SerializedName;
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
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.opensearch.response.error.ErrorMessage;
import org.opensearch.sql.opensearch.response.error.ErrorMessageFactory;
import org.opensearch.sql.protocol.response.QueryResult;

/**
 * Response formatter to format response to json format.
 */
public class JsonTypeResponseFormatter extends JsonResponseFormatter<QueryResult> {

  private final Style style;

  public JsonTypeResponseFormatter(Style style) {
    super(style);
    this.style = style;
  }

  @Override
  public Object buildJsonObject(QueryResult response) {
    JsonResponse.JsonResponseBuilder json = JsonResponse.builder();
    Total total = new Total(response.size(), "eq");
    Shards shards = new Shards(0, 0, 0, 0);
    List<String> columnNames = new LinkedList<>(response.columnNameTypes().keySet());
    List<Hit> hitList = new LinkedList<>();
    Map<String, Aggregation> aggregations = new LinkedHashMap<>();

    for (Object[] values : getRows(response)) {
      List<Object> valueList = new LinkedList<>(Arrays.asList(values));
      Map<String, Object> source = combineListsIntoMap(columnNames, valueList);

      Map<String, Object> filteredSource = filterSourceFromFunctions(source);
      Map<String, Object> functions = getFunctions(source);
      Map<String, Aggregation> aggregationMap = getAggregations(source);
      aggregations.putAll(aggregationMap);

      Hit hit = new Hit(filteredSource, functions.isEmpty() ? null : functions);

      hitList.add(hit);
    }
    Hits hits = new Hits(total, hitList, aggregations.isEmpty() ? null : aggregations);

    json.shards(shards).hits(hits);

    return json.build();
  }

  @Override
  public String format(Throwable t) {
    int status = getStatus(t);
    ErrorMessage message = ErrorMessageFactory.createErrorMessage(t, status);
    JdbcResponseFormatter.Error error = new JdbcResponseFormatter.Error(
            message.getType(),
            message.getReason(),
            message.getDetails());
    return jsonify(new JsonTypeErrorResponse(error, status));
  }

  private int getStatus(Throwable t) {
    return (t instanceof SyntaxCheckException
            || t instanceof QueryEngineException) ? 400 : 503;
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
    @SerializedName("_shards")
    public final Shards shards;
    public final Hits hits;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Hits {
    public final Total total;
    @Singular("column")
    public final List<Hit> hits;
    public final Map<String, Aggregation> aggregations;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Hit {
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

  @RequiredArgsConstructor
  @Getter
  public static class JsonTypeErrorResponse {
    private final JdbcResponseFormatter.Error error;
    private final int status;
  }
}
