/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;

public class NestedQuery extends LuceneQuery {
  @Override
  protected QueryBuilder doBuild(FunctionExpression predicate, ReferenceExpression path) {
    String fieldName = convertTextToKeyword(predicate.getArguments().get(0).toString(), predicate.getArguments().get(1).type());
    TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldName, value(predicate.getArguments().get(1).valueOf()));
    NestedQueryBuilder nestedQueryBuilder =  QueryBuilders.nestedQuery(path.toString(), termQuery, ScoreMode.None);
    return nestedQueryBuilder;
  }

  private Object value(ExprValue literal) {
    if (literal.type().equals(ExprCoreType.TIMESTAMP)) {
      return literal.timestampValue().toEpochMilli();
    } else {
      return literal.value();
    }
  }
}
