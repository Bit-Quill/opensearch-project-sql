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
    if (func.getFunctionName().getFunctionName().equalsIgnoreCase("nested")) { // Predicate
      ReferenceExpression nestedPath = (ReferenceExpression) func.getArguments().get(0);
      ReferenceExpression nestedField = (ReferenceExpression) ((FunctionExpression)func.getArguments().get(1)).getArguments().get(0);
      ExprValue literal = ((FunctionExpression)func.getArguments().get(1)).getArguments().get(1).valueOf();
      String fieldName = convertTextToKeyword(nestedField.toString(), nestedField.type()); // type?
      TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldName, value(literal));
      NestedQueryBuilder ret = QueryBuilders.nestedQuery(nestedPath.toString(), termQuery, ScoreMode.None);
      return ret;
    } else { // Syntax: 'WHERE nested(message.info) = 'a'
      ReferenceExpression field = (ReferenceExpression)((NestedFunction)func.getArguments().get(0)).getArguments().get(0);
      String fieldName = convertTextToKeyword(field.toString(), field.type());// function ret type?
      TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldName, value(func.getArguments().get(1).valueOf()));
      NestedQueryBuilder nestedQueryBuilder =  QueryBuilders.nestedQuery(getNestedPathString(field), termQuery, ScoreMode.None);
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