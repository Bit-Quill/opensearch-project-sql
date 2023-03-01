/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import java.util.Optional;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Aggregation;
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
      class VisitorContext {
        @Getter
        @Setter
        boolean isJSONSupported = true;
      }

      class JsonSupportedVisitor extends AbstractNodeVisitor<Boolean, VisitorContext> {
        @Override
        public Boolean visit(Node node, VisitorContext context) {
          // A node is supported if it's a leaf node or if all of its children are supported.
          return node.getChild().isEmpty()
              || node.getChild().stream().filter(c -> c.accept(this, context) != null)
              .allMatch(c -> c.accept(this, context));
        }

        @Override
        public Boolean visitAlias(Alias node, VisitorContext context) {
          // Alias node is accepted if it does not have a user-defined alias
          // and if the delegated expression is accepted.
          if (!StringUtils.isEmpty(node.getAlias()))
            return false;
          else
            return node.getDelegated().accept(this, context);
        }

        @Override
        public Boolean visitProject(Project node, VisitorContext context) {
          // Overridden visit functions are done in memory and are not supported with json format
          class UnsupportedExpressionsVisitor
              extends AbstractNodeVisitor<Boolean, Object> {

            @Override
            public Boolean visitFunction(Function node, Object context) {
              // queries with function calls are not supported.
              return false;
            }

            @Override
            public Boolean visitLiteral(Literal node, Object context) {
              // queries with literal values are not supported
              return false;
            }

            @Override
            public Boolean visitCast(Cast node, Object context) {
              // Queries with cast are not supported
              return false;
            }

            @Override
            public Boolean visitAlias(Alias node, Object context) {
              // Alias node is accepted if it does not have a user-defined alias
              // and if the delegated expression is accepted.
              if (!StringUtils.isEmpty(node.getAlias()))// && node.getDelegated().accept(this, context))
                return false;
              else {
                return node.getDelegated().accept(this, context);
              }
            }
          }

          // Project node is supported if all of its children are supported and
          // all expressions in project list are supported.
          UnsupportedExpressionsVisitor unsupportedExpressionsVisitor = new UnsupportedExpressionsVisitor();
          return visit(node, context)
              && node.getProjectList().stream().filter(c -> c.accept(unsupportedExpressionsVisitor, context) != null)
              .allMatch(e -> e.accept(unsupportedExpressionsVisitor, context));

        }

        @Override
        public Boolean visitAggregation(Aggregation node, VisitorContext context) {
          return node.getGroupExprList().isEmpty();
        }
      }

      Boolean isJsonSupported = ((Query) statement).getPlan().accept(new JsonSupportedVisitor(), new VisitorContext());

      if (!isJsonSupported) {
        throw new UnsupportedOperationException(
            "Queries with aggregation and in memory operations (cast, literals, alias, math, etc.)"
                + " are not yet supported with json format in the new engine");
      }
    }

    return queryExecutionFactory.create(statement, queryListener, explainListener);
  }
}
