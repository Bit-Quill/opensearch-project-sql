/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.opensearch.sql.opensearch.data.type.OpenSearchTextType.convertTextToKeyword;

import java.util.function.BiFunction;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;


public class NestedQuery extends LuceneQuery {
  @Override
  public QueryBuilder build(FunctionExpression func) {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    if (func.getArguments().get(0) instanceof ReferenceExpression) { // TODO can this be handled in just the else?
      applyInnerQuery(func, BoolQueryBuilder::filter, boolQuery);
    } else if (func.getArguments().get(0) instanceof FunctionExpression &&
        ((FunctionExpression)func.getArguments().get(0)).getFunctionName().getFunctionName().equalsIgnoreCase("nested")) { // Is predicate expression
      applyInnerQuery(func, BoolQueryBuilder::filter, boolQuery);
    }
    return boolQuery;
  }

  public QueryBuilder adInnerQuery(QueryBuilder builder, String path) {
//    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    NestedQueryBuilder nestedQueryBuilder = QueryBuilders.nestedQuery(path, builder, ScoreMode.None);
    return nestedQueryBuilder;
  }

  private QueryBuilder applyInnerQuery(FunctionExpression func, BiFunction<BoolQueryBuilder, QueryBuilder,
      QueryBuilder> accumulator, BoolQueryBuilder boolQuery) {
    // Syntax: 'WHERE nested(message.info) = 'a'
    // nestedFunctionAsPredicateExpression
    ReferenceExpression field = (ReferenceExpression)((FunctionExpression)func.getArguments().get(0)).getArguments().get(0);
    String fieldName = convertTextToKeyword(field.toString(), field.type());// function ret type?
    TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldName, value(func.getArguments().get(1).valueOf()));
    NestedQueryBuilder nestedQueryBuilder = QueryBuilders.nestedQuery(getNestedPathString(field), termQuery, ScoreMode.None);
    return accumulator.apply(boolQuery, nestedQueryBuilder);
    // TODO add range query and others that may apply...
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