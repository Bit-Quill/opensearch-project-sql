/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.protocol.response.format;

import static org.opensearch.sql.protocol.response.format.ErrorFormatter.prettyFormat;

import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.executor.Cursor;
import org.opensearch.sql.protocol.response.QueryResult;

public class OpenSearchJsonResponseFormatter implements ResponseFormatter<QueryResult> {

  private OpenSearchJsonResponse buildJsonObject(QueryResult response) {
    var builder = response.getCursor().equals(Cursor.None)
        ? OpenSearchJsonResponse.builder()
        : OpenSearchJsonCursorResponse.builder().cursor(response.getCursor().toString());

    return builder
        .took(response.getResponseMetadata().getTook())
        .timedOut(response.getResponseMetadata().isTimeOut())
        .shards(response.getResponseMetadata().getShards())
        .hits(Hits.builder()
            .total(new Total(response.getTotal()))
            .maxScore(response.getResponseMetadata().getMaxScore())
            .hits(response.getExprValues().stream()
                .map(ExprValueUtils::getTupleValue)
                .map(m -> m.entrySet().stream()
                    .collect(
                        HashMap<String, Object>::new,
                        (mm, v) -> mm.put(v.getKey(), v.getValue().value()),
                        HashMap::putAll))
// collector below crashes on null values, using code above to bypass this
//                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().value())))
                .map(m -> Hit.builder().fields(m).build())
                .toArray(Hit[]::new)
            )
            .build())
        .build();
  }

  @Override
  public String format(QueryResult response) {
    return AccessController.doPrivileged(
        (PrivilegedAction<String>) () -> new GsonBuilder()
            .setPrettyPrinting()
            .disableHtmlEscaping()
            .serializeNulls()
            .create()
            .toJson(buildJsonObject(response)));
  }

  @Override
  public String format(Throwable t) {
    return AccessController.doPrivileged((PrivilegedAction<String>) () -> prettyFormat(t));
  }

  // Using another class with another build to ensure that cursor is not serialized
  // when not given. GSON has no option to exclude a specific field when it is null.
  @Getter
  @SuperBuilder
  public static class OpenSearchJsonCursorResponse extends OpenSearchJsonResponse {
    @SerializedName(value = "_scroll_id")
    private final String cursor;
  }

  @Getter
  @SuperBuilder
  public static class OpenSearchJsonResponse {
    protected final long took;
    @SerializedName(value = "timed_out")
    protected final boolean timedOut;
    @SerializedName(value = "_shards")
    protected final ExecutionEngine.ResponseMetadata.Shards shards;
    protected final Hits hits;
  }

  @Getter
  @Builder
  public static class Hits {
    private final Total total;
    @SerializedName(value = "max_score")
    private final long maxScore;
    private final Hit[] hits;
  }

  @Getter
  @RequiredArgsConstructor
  public static class Total {
    private final long value;
    private final String relation = "eq";
  }

  @Getter
  @Builder
  public static class Hit {
    @SerializedName(value = "_index")
    private final String index;
    @SerializedName(value = "_id")
    private final String id;
    @SerializedName(value = "_score")
    private final double score;
    private final Map<String, Object> fields;
  }
}
