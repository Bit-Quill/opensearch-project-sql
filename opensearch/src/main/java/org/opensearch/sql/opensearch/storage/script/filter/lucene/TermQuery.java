/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;

import static org.opensearch.sql.opensearch.data.type.OpenSearchTextType.convertTextToKeyword;

/**
 * Lucene query that build term query for equality comparison.
 */
public class TermQuery extends LuceneQuery {

  @Override
  protected QueryBuilder doBuild(String fieldName, ExprType fieldType, ExprValue literal) {
    fieldName = convertTextToKeyword(fieldName, fieldType);
    return QueryBuilders.termQuery(fieldName, value(literal));
  }

  @Override
  protected QueryBuilder doBuildNested(FunctionExpression field, ExprValue literal) {
    // TODO Can we move this to NestedQuery::doBuild()?
    return QueryBuilders.nestedQuery(getNestedPathString((ReferenceExpression) field.getArguments().get(0)),
        doBuild(field.getArguments().get(0).toString(), field.getArguments().get(0).type(), literal), ScoreMode.None);
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
