/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;

public class LikeQuery extends LuceneQuery {
  @Override
  public QueryBuilder build(FunctionExpression func) {
    ReferenceExpression ref = (ReferenceExpression) func.getArguments().get(0);
    Expression expr = func.getArguments().get(1);
    ExprValue literalValue = expr.valueOf();

    return createBuilder(ref.toString(), literalValue.stringValue());
  }

  protected WildcardQueryBuilder createBuilder(String field, String query) {
    String matchText = StringUtils.convertSqlWildcardToLucene(query);
    return QueryBuilders.wildcardQuery(field, matchText);
  }
}
