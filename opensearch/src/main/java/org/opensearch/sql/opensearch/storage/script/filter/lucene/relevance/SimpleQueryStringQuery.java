/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import java.util.List;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;

public class SimpleQueryStringQuery extends LuceneQuery {

  @Override
  public QueryBuilder build(FunctionExpression func) {
    List<Expression> arguments = func.getArguments();
    Expression fields = arguments.get(0);
    Expression query = arguments.get(1);
    SimpleQueryStringBuilder queryBuilder = QueryBuilders.simpleQueryStringQuery(
        query.valueOf(null).stringValue());
    String fieldsValue = fields.valueOf(null).stringValue();
    if (!fieldsValue.equals("*")) {
      throw new SemanticCheckException("Only * is supported");
    }
    queryBuilder.field(fieldsValue);
    return queryBuilder;
  }
}
