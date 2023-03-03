/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import java.util.Optional;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.analysis.JsonSupportAnalyzer;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.sql.sql.parser.AstBuilder;
import org.opensearch.sql.sql.parser.AstStatementBuilder;

/**
 * SQL service.
 */
@RequiredArgsConstructor
public class SQLService {

  private final SQLSyntaxParser parser;

  private final QueryManager queryManager;

  private final QueryPlanFactory queryExecutionFactory;

  /**
   * Given {@link SQLQueryRequest}, execute it. Using listener to listen result.
   *
   * @param request {@link SQLQueryRequest}
   * @param listener callback listener
   */
  public void execute(SQLQueryRequest request, ResponseListener<QueryResponse> listener) {
    try {
      queryManager.submit(plan(request, Optional.of(listener), Optional.empty()));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Given {@link SQLQueryRequest}, explain it. Using listener to listen result.
   *
   * @param request {@link SQLQueryRequest}
   * @param listener callback listener
   */
  public void explain(SQLQueryRequest request, ResponseListener<ExplainResponse> listener) {
    try {
      queryManager.submit(plan(request, Optional.empty(), Optional.of(listener)));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private AbstractPlan plan(
      SQLQueryRequest request,
      Optional<ResponseListener<QueryResponse>> queryListener,
      Optional<ResponseListener<ExplainResponse>> explainListener) {
    if (parser.containsHints(request.getQuery()))
      throw new UnsupportedOperationException("Hints are not yet supported in the new engine.");

    // 1.Parse query and convert parse tree (CST) to abstract syntax tree (AST)
    ParseTree cst = parser.parse(request.getQuery());
    Statement statement =
        cst.accept(
            new AstStatementBuilder(
                new AstBuilder(request.getQuery()),
                AstStatementBuilder.StatementBuilderContext.builder()
                    .isExplain(request.isExplainRequest())
                    .build()));

    // There is no full support for JSON format yet for in memory operations, aliases, literals, and casts
    // Aggregation has differences with legacy results
    if (request.format().getFormatName().equals("json")) {
      Boolean isJsonSupported = ((Query) statement).getPlan().accept(new JsonSupportAnalyzer(), null);

      if (!isJsonSupported) {
        throw new UnsupportedOperationException(
            "Queries with aggregation and in memory operations (cast, literals, alias, math, etc.)"
                + " are not yet supported with json format in the new engine");
      }
    }

    return queryExecutionFactory.create(statement, queryListener, explainListener);
  }
}
