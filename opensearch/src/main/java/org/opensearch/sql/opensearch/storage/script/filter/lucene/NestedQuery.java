/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.opensearch.sql.opensearch.data.type.OpenSearchTextType.convertTextToKeyword;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;


public class NestedQuery extends LuceneQuery {
  @Override
  protected QueryBuilder doBuild(FunctionExpression func) {
    if (true) { // Predicate
      ExprValue literalValue = func.getArguments().get(1).valueOf();
      return QueryBuilders.nestedQuery(getNestedPathString((ReferenceExpression) func.getArguments().get(0)),
          doBuild(func.getArguments().get(0).toString(), func.getArguments().get(0).type(), literalValue), ScoreMode.None);
    } else { // Plain Func
      String fieldName = convertTextToKeyword(func.getArguments().get(0).toString(), func.getArguments().get(1).type());
      TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldName, value(func.getArguments().get(1).valueOf()));
      NestedQueryBuilder nestedQueryBuilder =  QueryBuilders.nestedQuery(func.getArguments().get(0).toString(), termQuery, ScoreMode.None);
      return nestedQueryBuilder;
    }
  }

  private String getNestedPathString(ReferenceExpression field) {
    String ret = "";
    for (int i = 0; i < field.getPaths().size() - 1; i++) {
      ret +=  (i == 0) ? field.getPaths().get(i) : "." + field.getPaths().get(i);
    }
    return ret;
  }

  private Object value(ExprValue literal) {
    if (literal.type().equals(ExprCoreType.TIMESTAMP)) {
      return literal.timestampValue().toEpochMilli();
    } else {
      return literal.value();
    }
  }
}