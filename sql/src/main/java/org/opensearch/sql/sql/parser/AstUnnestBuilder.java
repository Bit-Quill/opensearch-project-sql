/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Unnest;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;
import org.opensearch.sql.sql.parser.context.QuerySpecification;

@RequiredArgsConstructor
public class AstUnnestBuilder extends OpenSearchSQLParserBaseVisitor<UnresolvedPlan> {

  /**
   * Query specification that contains info collected beforehand.
   */
  private final QuerySpecification querySpec;

  @Override
  public UnresolvedPlan visit(ParseTree selectClause) {
    Unnest unnest = null;
    for (UnresolvedExpression item : querySpec.getSelectItems()) {
      // Assume is nested as that's all that's currently supported as alias in SELECT clause from AstBuilder
      if (item instanceof Alias) {
        Function func = (Function)((Alias)item).getDelegated();
        validateArgs(func);
        if (unnest == null) {
          unnest = new Unnest(func);
        } else {
          unnest.add(func);
        }
      }
    }
    return unnest;
  }

  /**
   * Ensure any nested functions used in SELECT statement do not have 'condition' parameter used.
   * @param nestedFunction : Nested function call.
   */
  private void validateArgs(Function nestedFunction) {
    nestedFunction.getFuncArgs().stream().filter(
        arg -> arg instanceof Function
        ).findAny().ifPresent(e -> {
          throw new SemanticCheckException("Condition parameter for the nested " +
              "function is invalid in a SELECT statement: 'nested(field | field, path)'");
        });
  }
}
