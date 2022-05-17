/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.SimpleQueryStringFlag;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class SimpleQueryString extends LuceneQuery {
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> analyzeWildcard =
      (b, v) -> b.analyzeWildcard(Boolean.parseBoolean(v.stringValue()));
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> analyzer =
      (b, v) -> b.analyzer(v.stringValue());
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> autoGenerateSynonymsPhraseQuery =
      (b, v) -> b.autoGenerateSynonymsPhraseQuery(Boolean.parseBoolean(v.stringValue()));
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> defaultOperator =
      (b, v) -> b.defaultOperator(Operator.fromString(v.stringValue()));
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> flags =
      (b, v) -> b.flags(SimpleQueryStringFlag.valueOf(v.stringValue()));
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> fuzzyMaxExpansions =
      (b, v) -> b.fuzzyMaxExpansions(Integer.parseInt(v.stringValue()));
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> fuzzyPrefixLength =
      (b, v) -> b.fuzzyPrefixLength(Integer.parseInt(v.stringValue()));
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> fuzzyTranspositions =
      (b, v) -> b.fuzzyTranspositions(Boolean.parseBoolean(v.stringValue()));
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> lenient =
      (b, v) -> b.lenient(Boolean.parseBoolean(v.stringValue()));
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> minimumShouldMatch =
      (b, v) -> b.minimumShouldMatch(v.stringValue());
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> quoteFieldSuffix =
      (b, v) -> b.quoteFieldSuffix(v.stringValue());
  private final BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder> boost =
      (b, v) -> b.boost(Float.parseFloat(v.stringValue()));

  ImmutableMap<Object, Object> argAction = ImmutableMap.builder()
      .put("analyze_wildcard", analyzeWildcard)
      .put("analyzer", analyzer)
      .put("auto_generate_synonyms_phrase_query", autoGenerateSynonymsPhraseQuery)
      .put("flags", flags)
      .put("fuzzy_max_expansions", fuzzyMaxExpansions)
      .put("fuzzy_prefix_length", fuzzyPrefixLength)
      .put("fuzzy_transpositions", fuzzyTranspositions)
      .put("lenient", lenient)
      .put("default_operator", defaultOperator)
      .put("minimum_should_match", minimumShouldMatch)
      .put("quote_field_suffix", quoteFieldSuffix)
      .put("boost", boost)
      .build();

  @Override
  public QueryBuilder build(FunctionExpression func) {
    Iterator<Expression> iterator = func.getArguments().iterator();
    var fields = (NamedArgumentExpression) iterator.next();
    var query = (NamedArgumentExpression) iterator.next();
    SimpleQueryStringBuilder queryBuilder = QueryBuilders
      .simpleQueryStringQuery(query.getValue().valueOf(null).stringValue())
      .fields(parseFields(fields.getValue().valueOf(null).collectionValue()));
    while (iterator.hasNext()) {
      NamedArgumentExpression arg = (NamedArgumentExpression) iterator.next();
      if (!argAction.containsKey(arg.getArgName())) {
        throw new SemanticCheckException(String
            .format("Parameter %s is invalid for simple_query_string function.", arg.getArgName()));
      }
      ((BiFunction<SimpleQueryStringBuilder, ExprValue, SimpleQueryStringBuilder>) argAction
          .get(arg.getArgName()))
          .apply(queryBuilder, arg.getValue().valueOf(null));
    }
    return queryBuilder;
  }

  private Map<String, Float> parseFields(List<ExprValue> input) {
    try {
      var res = new HashMap<String, Float>();
      for (var literal : input) {
        var elem = literal.stringValue();
        if (!elem.contains("^")) {
          res.put(elem, 1F);
          continue;
        }

        var parts = elem.split("\\^");
        var weight = Float.parseFloat(parts[parts.length - 1]);
        var field = Arrays.stream(parts).limit(parts.length - 1).collect(Collectors.joining("^"));
        res.put(field, weight);
      }
      return res;
    }
    catch (Exception e) {
      throw new SemanticCheckException(String.format(
              "%s: Incorrect value specified for 'fields' argument of 'simple_query_string' function. "
              + "The format is: '[\"field1\", \"field2\", ...]'.", e.getMessage()));
    }
  }
}
