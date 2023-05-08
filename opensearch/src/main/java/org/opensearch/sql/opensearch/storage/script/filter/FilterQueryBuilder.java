/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter;

import static java.util.Collections.emptyMap;
import static org.opensearch.script.Script.DEFAULT_SCRIPT_TYPE;
import static org.opensearch.sql.opensearch.storage.script.ExpressionScriptEngine.EXPRESSION_LANG_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LikeQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.RangeQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.RangeQuery.Comparison;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.TermQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchBoolPrefixQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchPhrasePrefixQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchPhraseQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MultiMatchQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.QueryQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.QueryStringQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.SimpleQueryStringQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.WildcardQuery;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

@RequiredArgsConstructor
public class FilterQueryBuilder extends ExpressionNodeVisitor<QueryBuilder, Object> {

  /**
   * Serializer that serializes expression for build DSL query.
   */
  private final ExpressionSerializer serializer;

  /**
   * Mapping from function name to lucene query builder.
   */
  private final Map<FunctionName, LuceneQuery> luceneQueries =
      ImmutableMap.<FunctionName, LuceneQuery>builder()
          .put(BuiltinFunctionName.EQUAL.getName(), new TermQuery())
          .put(BuiltinFunctionName.LESS.getName(), new RangeQuery(Comparison.LT))
          .put(BuiltinFunctionName.GREATER.getName(), new RangeQuery(Comparison.GT))
          .put(BuiltinFunctionName.LTE.getName(), new RangeQuery(Comparison.LTE))
          .put(BuiltinFunctionName.GTE.getName(), new RangeQuery(Comparison.GTE))
          .put(BuiltinFunctionName.LIKE.getName(), new LikeQuery())
          .put(BuiltinFunctionName.MATCH.getName(), new MatchQuery())
          .put(BuiltinFunctionName.MATCH_PHRASE.getName(), new MatchPhraseQuery())
          .put(BuiltinFunctionName.MATCHPHRASE.getName(), new MatchPhraseQuery())
          .put(BuiltinFunctionName.MATCHPHRASEQUERY.getName(), new MatchPhraseQuery())
          .put(BuiltinFunctionName.QUERY.getName(), new QueryQuery())
          .put(BuiltinFunctionName.MATCH_QUERY.getName(), new MatchQuery())
          .put(BuiltinFunctionName.MATCHQUERY.getName(), new MatchQuery())
          .put(BuiltinFunctionName.MULTI_MATCH.getName(), new MultiMatchQuery())
          .put(BuiltinFunctionName.MULTIMATCH.getName(), new MultiMatchQuery())
          .put(BuiltinFunctionName.MULTIMATCHQUERY.getName(), new MultiMatchQuery())
          .put(BuiltinFunctionName.SIMPLE_QUERY_STRING.getName(), new SimpleQueryStringQuery())
          .put(BuiltinFunctionName.QUERY_STRING.getName(), new QueryStringQuery())
          .put(BuiltinFunctionName.MATCH_BOOL_PREFIX.getName(), new MatchBoolPrefixQuery())
          .put(BuiltinFunctionName.MATCH_PHRASE_PREFIX.getName(), new MatchPhrasePrefixQuery())
          .put(BuiltinFunctionName.WILDCARD_QUERY.getName(), new WildcardQuery())
          .put(BuiltinFunctionName.WILDCARDQUERY.getName(), new WildcardQuery())
          .build();

  /**
   * Build OpenSearch filter query from expression.
   * @param expr  expression
   * @return      query
   */
  public QueryBuilder build(Expression expr) {
    return expr.accept(this, null);
  }

  @Override
  public QueryBuilder visitFunction(FunctionExpression func, Object context) {
    FunctionName name = func.getFunctionName();
    switch (name.getFunctionName()) {
      case "and":
        return buildBoolQuery(func, context, BoolQueryBuilder::filter);
      case "or":
        return buildBoolQuery(func, context, BoolQueryBuilder::should);
      case "not":
        return buildBoolQuery(func, context, BoolQueryBuilder::mustNot);
      case "nested":
        // nested (field, condition)
        // example: WHERE message.info = 'a' OR message.info = 'b'
        // => bool { must { bool { should { term(message.info, 'a') , term(message.info, 'b') } } }

        // example: WHERE nested(message.info) = 'a' OR nested(message.info) = 'b'
        // => bool { must { bool { should { term( nested { term { message.info} } }, 'a') , term( nested { term { message.info} } }, 'b') } } }
        // => bool { must { bool { nested { bool { must { term( message.info, 'a') , term( message.info, 'b') } } }

        // example: WHERE nested(message.info, message.info = 'a' OR message.info = 'b' ...)
        // => bool { must { bool { nested { bool { must { term( message.info, 'a') , term( message.info, 'b') } } }

        // => bool { must { nested { bool { should { term() , term() } } } }
        // example: WHERE nested(message.info, message.age > 20 OR message.info = 'b' ...)
        // example: WHERE nested(message.info.name, message.info.name = 'a') OR nested(message.info.address, message.info.address = 'b')

        // WHERE nested(message, message.info.name, message.info.name = "Andrew" OR message.comment = "SECOND")
        // WHERE (nested(message.info.name) = "Andrew" AND nested(message.comment) = "FIRST") OR (nested(message.info.name) = "Guian" AND nested(message.comment = "SECOND"))

        // example: WHERE nested(foo.bar, nested(zoo.blah, condition))

        if (func.getArguments().size() > 1) {
          Expression secondArgument = func.getArguments().get(1);
          if (secondArgument instanceof FunctionExpression) {
            ReferenceExpression path = (ReferenceExpression)func.getArguments().get(0);
            FunctionExpression condition = (FunctionExpression)func.getArguments().get(1);
            QueryBuilder queryBuilder = visitFunction(condition, context);
            NestedQueryBuilder
                nestedQueryBuilder = QueryBuilders.nestedQuery(path.toString(), queryBuilder, ScoreMode.None);
            return nestedQueryBuilder;
          }
        }
        // else, this doesn't have a condition so we need to just call nested without a queryBuilder
      default: {
        LuceneQuery query = luceneQueries.get(name);
        if (query != null && query.canSupport(func)) {
          return query.build(func);
        } else if (query != null && query.isNestedFunction(func)) {
          QueryBuilder outerQuery = query.buildNested(func);
          boolean hasPathParam = (((FunctionExpression)func.getArguments().get(0)).getArguments().size() == 2);
          String pathStr = !hasPathParam ?
              getNestedPathString((ReferenceExpression) ((FunctionExpression)func.getArguments().get(0)).getArguments().get(0)) :
              ((FunctionExpression)func.getArguments().get(0)).getArguments().get(0).toString();
          return QueryBuilders.nestedQuery(pathStr, outerQuery, ScoreMode.None);
        }

        return buildScriptQuery(func);
      }
    }
  }

  private boolean funcArgsIsPredicateExpression(FunctionExpression func) {
    func.getArguments().stream().forEach(
        a -> {
          if (a instanceof FunctionExpression) {
            funcArgsIsPredicateExpression((FunctionExpression) a);
          }
        }
    );
    return false;
  }

  private String getNestedPathString(ReferenceExpression field) {
    String ret = "";
    for (int i = 0; i < field.getPaths().size() - 1; i++) {
      ret +=  (i == 0) ? field.getPaths().get(i) : "." + field.getPaths().get(i);
    }
    return ret;
  }

  private BoolQueryBuilder buildBoolQuery(FunctionExpression node,
                                          Object context,
                                          BiFunction<BoolQueryBuilder, QueryBuilder,
                                              QueryBuilder> accumulator) {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    for (Expression arg : node.getArguments()) {
      accumulator.apply(boolQuery, arg.accept(this, context));
    }
    return boolQuery;
  }

  private ScriptQueryBuilder buildScriptQuery(FunctionExpression node) {
    return new ScriptQueryBuilder(new Script(
        DEFAULT_SCRIPT_TYPE, EXPRESSION_LANG_NAME, serializer.serialize(node), emptyMap()));
  }

}
