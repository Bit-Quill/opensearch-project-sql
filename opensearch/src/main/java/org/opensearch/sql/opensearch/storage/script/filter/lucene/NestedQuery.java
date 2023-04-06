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
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;


public class NestedQuery extends LuceneQuery {
  @Override
  public QueryBuilder build(FunctionExpression func) {
//    FunctionExpression funcOrMultipleFuncs = (FunctionExpression) func.getArguments().get(1);
//    if (func.getFunctionName().getFunctionName().equalsIgnoreCase("nested")) {
//      switch (funcOrMultipleFuncs.getFunctionName().getFunctionName()) {
//        case "and":
//        case "or":
//        case "not":
//          for (var f : func.getArguments()) {
//            switch (funcOrMultipleFuncs.getFunctionName().getFunctionName()) {
//              case "and":
//                return otherRetFunc(func, BoolQueryBuilder::should);
//              case "or":
//                return otherRetFunc(func, BoolQueryBuilder::should);
//              case "not":
//                return otherRetFunc(func, BoolQueryBuilder::mustNot);
//              default:
//            }
//          }
//          break;
//
//        default:
//          return doBuild(func, BoolQueryBuilder::filter);
//      }
//    }
//    return otherRetFunc(func, BoolQueryBuilder::filter);


    List<QueryBuilder> queries = new ArrayList<>();
    if (true) {
        FunctionExpression expr = (FunctionExpression) ((FunctionExpression)func.getArguments().get(1)).getArguments().get(i);
    } else if (((FunctionExpression)func.getArguments().get(0)).getFunctionName().getFunctionName().equalsIgnoreCase("nested")) { // Is predicate expression
      // TODO If function doesnt contains conditional we should throw exception.
      for (int i = 0; i < ((FunctionExpression)func.getArguments().get(1)).getArguments().size(); i++) {
        FunctionExpression expr = (FunctionExpression) ((FunctionExpression)func.getArguments().get(1)).getArguments().get(i);
        queries.add(otherRetFunc(func, BoolQueryBuilder::filter));
      }
      return boolQuery;
    }
      // Conditional with only one predicate.
      if (((FunctionExpression)func.getArguments().get(1)).getArguments().get(0) instanceof ReferenceExpression
          && ((FunctionExpression)func.getArguments().get(1)).getArguments().get(1) instanceof LiteralExpression) {
        queries.add(doBuild((FunctionExpression)func.getArguments().get(1), path));
      } else {
        // Multiple predicates. Only enters if more than one predicate.
        for (int i = 0; i < ((FunctionExpression)func.getArguments().get(1)).getArguments().size(); i++) {
          FunctionExpression expr = (FunctionExpression) ((FunctionExpression)func.getArguments().get(1)).getArguments().get(i);
          queries.add(doBuild(expr, path));
        }
      }
      for (QueryBuilder query : queries) {
        accumulator.apply(boolQuery, query);
      }
    return boolQuery;

  }

  private QueryBuilder otherRetFunc(FunctionExpression func, BiFunction<BoolQueryBuilder, QueryBuilder,
        QueryBuilder> accumulator) {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    if (func.getFunctionName().getFunctionName().equalsIgnoreCase("nested")) { // Predicate
      ReferenceExpression nestedPath = (ReferenceExpression) func.getArguments().get(0);
      ReferenceExpression nestedField = (ReferenceExpression) ((FunctionExpression)func.getArguments().get(1)).getArguments().get(0);
      ExprValue literal = ((FunctionExpression)func.getArguments().get(1)).getArguments().get(1).valueOf();
      String fieldName = convertTextToKeyword(nestedField.toString(), nestedField.type()); // type?
      TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldName, value(literal));
      NestedQueryBuilder ret = QueryBuilders.nestedQuery(nestedPath.toString(), termQuery, ScoreMode.None);
      return accumulator.apply(boolQuery, ret);
    } else { // Syntax: 'WHERE nested(message.info) = 'a'
      ReferenceExpression field = (ReferenceExpression)((FunctionExpression)func.getArguments().get(0)).getArguments().get(0);
      String fieldName = convertTextToKeyword(field.toString(), field.type());// function ret type?
      TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldName, value(func.getArguments().get(1).valueOf()));
      NestedQueryBuilder nestedQueryBuilder =  QueryBuilders.nestedQuery(getNestedPathString(field), termQuery, ScoreMode.None);
//      return nestedQueryBuilder;
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