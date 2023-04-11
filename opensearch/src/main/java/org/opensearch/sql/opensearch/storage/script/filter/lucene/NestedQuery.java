/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.opensearch.sql.opensearch.data.type.OpenSearchTextType.convertTextToKeyword;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;


public class NestedQuery extends LuceneQuery {
  @Override
  public QueryBuilder build(FunctionExpression func) {
    // TODO If function doesnt contains conditional we should throw exception.
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    // WHERE nested(message, message.info = '' AND comment.data = '')
    if (func.getFunctionName().getFunctionName().equalsIgnoreCase(BuiltinFunctionName.NESTED.name())) {
      // TODO recursion
      for (Expression arg : ((FunctionExpression)func.getArguments().get(1)).getArguments()) {
        switch (((FunctionExpression) arg).getFunctionName().getFunctionName()) {
          case "and":
            otherRetFunc((FunctionExpression) arg, BoolQueryBuilder::must, boolQuery);
            break;
          case "or":
            otherRetFunc((FunctionExpression) arg, BoolQueryBuilder::should, boolQuery);
            break;
          case "not":
            otherRetFunc((FunctionExpression) arg, BoolQueryBuilder::mustNot, boolQuery);
            break;
          default:
            otherRetFunc((FunctionExpression) arg, BoolQueryBuilder::filter, boolQuery);
        }
      }
      // Default way
    } else if (((FunctionExpression)func.getArguments().get(0)).getFunctionName().getFunctionName().equalsIgnoreCase("nested")) { // Is predicate expression
      otherRetFunc(func, BoolQueryBuilder::filter, boolQuery);
    }
    return boolQuery;

  }

  private QueryBuilder otherRetFunc(FunctionExpression func, BiFunction<BoolQueryBuilder, QueryBuilder,
        QueryBuilder> accumulator, BoolQueryBuilder boolQuery) {
    if (func.getFunctionName().getFunctionName().equalsIgnoreCase("nested")) { // Predicate
      ReferenceExpression nestedPath = (ReferenceExpression) func.getArguments().get(0);
      ReferenceExpression nestedField = (ReferenceExpression) ((FunctionExpression)func.getArguments().get(1)).getArguments().get(0);
      ExprValue literal = ((FunctionExpression)func.getArguments().get(1)).getArguments().get(1).valueOf();
      String fieldName = convertTextToKeyword(nestedField.toString(), nestedField.type()); // type?
      TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldName, value(literal));
      NestedQueryBuilder ret = QueryBuilders.nestedQuery(nestedPath.toString(), termQuery, ScoreMode.None);
      return accumulator.apply(boolQuery, ret);
    } else if (func.getArguments().get(0) instanceof ReferenceExpression) {
      ReferenceExpression field = (ReferenceExpression)func.getArguments().get(0);
      String fieldName = convertTextToKeyword(field.toString(), field.type());// function ret type?
      TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldName, value(func.getArguments().get(1).valueOf()));
      NestedQueryBuilder nestedQueryBuilder =  QueryBuilders.nestedQuery(getNestedPathString(field), termQuery, ScoreMode.None);
      return accumulator.apply(boolQuery, nestedQueryBuilder);
    } else { // Syntax: 'WHERE nested(message.info) = 'a'
      ReferenceExpression field = (ReferenceExpression)((FunctionExpression)func.getArguments().get(0)).getArguments().get(0);
      String fieldName = convertTextToKeyword(field.toString(), field.type());// function ret type?
      TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldName, value(func.getArguments().get(1).valueOf()));
      NestedQueryBuilder nestedQueryBuilder =  QueryBuilders.nestedQuery(getNestedPathString(field), termQuery, ScoreMode.None);
      return accumulator.apply(boolQuery, nestedQueryBuilder);
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