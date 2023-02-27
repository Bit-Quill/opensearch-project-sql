/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Project;
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
    // Hints are currently unsupported for V2
    ParseTree cstHints = parser.parseHints(request.getQuery());
    if (cstHints.getChildCount() > 1)
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
      List projectList = ((Project) ((Query) statement).getPlan()).getProjectList();
      List planChildren = ((Project) ((Query) statement).getPlan()).getChild();
      UnsupportedOperationException aggException = new UnsupportedOperationException(
          "Queries with aggregation are not yet supported with json format in the new engine");

      for (var child : planChildren) {
        if (child instanceof Aggregation && ((Aggregation) child).getGroupExprList().size() > 0)
          throw aggException;

        if (child instanceof Filter) {
          for (var filterChild : ((Filter) child).getChild())
            if (filterChild instanceof Aggregation)
              throw aggException;
        }

        if (child instanceof Limit) {
          for (var filterChild : ((Limit) child).getChild())
            if(filterChild instanceof Aggregation)
              throw aggException;
        }
      }

      for (var project : projectList) {
        if (project instanceof Alias) {
          Alias projectAlias = ((Alias) project);

          if (projectAlias.getDelegated() instanceof Function ||
              projectAlias.getDelegated() instanceof Literal ||
              projectAlias.getDelegated() instanceof Cast ||
              (projectAlias.getAlias() != null && !projectAlias.getAlias().isEmpty()))
            throw new UnsupportedOperationException("The query is not yet supported with json "
                + "format because there is an expression in the query that is not pushed down to the OpenSearch instance");
        }
      }
    }

    return queryExecutionFactory.create(statement, queryListener, explainListener);
  }
}
