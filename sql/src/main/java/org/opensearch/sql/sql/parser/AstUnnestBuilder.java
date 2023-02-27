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
      if (item instanceof Alias
          && ((Alias)item).getDelegated() instanceof Function
          && ((Function)((Alias)item).getDelegated()).getFuncName().equalsIgnoreCase("nested")) {
        if (unnest == null) {
          unnest = buildUnnest((Function)((Alias)item).getDelegated());
        } else {
          unnest.add((Function)((Alias)item).getDelegated());
        }
      }
    }
    return unnest;
  }

  private Unnest buildUnnest(Function nestedFunction) {
    return new Unnest(nestedFunction);
  }
}
