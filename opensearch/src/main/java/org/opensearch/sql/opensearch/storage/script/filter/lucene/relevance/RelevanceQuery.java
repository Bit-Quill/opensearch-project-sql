/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import lombok.RequiredArgsConstructor;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;

/**
 * Base class for query abstraction that builds a relevance query from function expression.
 */
@RequiredArgsConstructor
public abstract class RelevanceQuery<T extends QueryBuilder> extends LuceneQuery {
  private final String queryName;
  private final Map<String, QueryBuilderStep<T>> queryBuildActions;

  @Override
  public QueryBuilder build(FunctionExpression func) {
    List<Expression> arguments = func.getArguments();
    if (arguments.size() < 2) {
      throw new SyntaxCheckException(
          String.format("%s requires at least two parameters", queryName));
    }
    NamedArgumentExpression field = (NamedArgumentExpression) arguments.get(0);
    NamedArgumentExpression query = (NamedArgumentExpression) arguments.get(1);
    T queryBuilder = createQueryBuilder(field, query);

    Iterator<Expression> iterator = arguments.listIterator(2);
    while (iterator.hasNext()) {
      NamedArgumentExpression arg = (NamedArgumentExpression) iterator.next();
      if (!queryBuildActions.containsKey(arg.getArgName())) {
        throw new SemanticCheckException(
            String.format("Parameter %s is invalid for %s function.",
                arg.getArgName(), queryBuilder.getWriteableName()));
      }
      (Objects.requireNonNull(
          queryBuildActions
              .get(arg.getArgName())))
          .apply(queryBuilder, arg.getValue().valueOf(null));
    }
    return queryBuilder;
  }

  protected abstract T createQueryBuilder(NamedArgumentExpression field,
                                          NamedArgumentExpression query);

  /**
   * Convenience interface for a function that updates a QueryBuilder
   * based on ExprValue.
   *
   * @param <T> Concrete query builder
   */
  protected interface QueryBuilderStep<T extends QueryBuilder> extends
      BiFunction<T, ExprValue, T> {
  }
}
