/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.executor.Cursor;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Execution engine that encapsulates execution details.
 */
public interface ExecutionEngine {

  /**
   * Execute physical plan and call back response listener.
   * Todo. deprecated this interface after finalize {@link ExecutionContext}.
   *
   * @param plan     executable physical plan
   * @param listener response listener
   */
  void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener);

  /**
   * Execute physical plan with {@link ExecutionContext} and call back response listener.
   */
  void execute(PhysicalPlan plan, ExecutionContext context,
               ResponseListener<QueryResponse> listener);

  /**
   * Explain physical plan and call back response listener. The reason why this has to
   * be part of execution engine interface is that the physical plan probably needs to
   * be executed to get more info for profiling, such as actual execution time, rows fetched etc.
   *
   * @param plan     physical plan to explain
   * @param listener response listener
   */
  void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener);

  /**
   * Data class that encapsulates ExprValue.
   */
  @Data
  @RequiredArgsConstructor
  class QueryResponse {
    private final Schema schema;
    private final List<ExprValue> results;
    private final long total;
    private final Cursor cursor;
    private final ResponseMetadata responseMetadata;

    /**
     * Constructor for Query Response.
     *
     * @param schema  schema of the query
     * @param results list of expressions
     */
    public QueryResponse(Schema schema, List<ExprValue> results) {
      this.schema = schema;
      this.results = results;
      this.total = 0;
      this.cursor = null;
      this.responseMetadata = new ResponseMetadata();
    }
  }

  @Data
  class Schema {
    private final List<Column> columns;

    @Data
    public static class Column {
      private final String name;
      private final String alias;
      private final ExprType exprType;
    }
  }

  @Data
  @Accessors(chain = true)
  class ResponseMetadata {
    private long took = 0;
    private boolean timeOut = false;
    private long maxScore = 1; //or double?
    private Shards shards = new Shards();

    @Data
    @Accessors(chain = true)
    public static class Shards {
      private long total = 0;
      private long successful = 0;
      private long skipped = 0;
      private long failed = 0;

      public Shards() {
      }
    }

    public ResponseMetadata() {
    }
  }

  /**
   * Data class that encapsulates explain result. This can help decouple core engine
   * from concrete explain response format.
   */
  @Data
  class ExplainResponse {
    private final ExplainResponseNode root;
  }

  @AllArgsConstructor
  @Data
  @RequiredArgsConstructor
  class ExplainResponseNode {
    private final String name;
    private Map<String, Object> description;
    private List<ExplainResponseNode> children;
  }

}
