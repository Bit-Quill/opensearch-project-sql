/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.Objects;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;

/**
 * Class for Lucene query that builds the query_string query.
 */
public class QueryStringQuery extends MultiFieldQuery<QueryStringQueryBuilder> {
  /**
   *  Default constructor for QueryString configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public QueryStringQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<QueryStringQueryBuilder>>builder()
        .put("allow_leading_wildcard", (b, v) -> b.allowLeadingWildcard(
            FunctionParameterRepository.convertBoolValue(v, "allow_leading_wildcard")))
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("analyze_wildcard", (b, v) -> b.analyzeWildcard(
            FunctionParameterRepository.convertBoolValue(v, "analyze_wildcard")))
        .put("auto_generate_synonyms_phrase_query", (b, v) -> b.autoGenerateSynonymsPhraseQuery(
            FunctionParameterRepository.convertBoolValue(v,"auto_generate_synonyms_phrase_query")))
        .put("boost", (b, v) -> b.boost(
            FunctionParameterRepository.convertFloatValue(v, "boost")))
        .put("default_operator", (b, v) -> b.defaultOperator(
            FunctionParameterRepository.convertOperator(v, "default_operator")))
        .put("enable_position_increments", (b, v) -> b.enablePositionIncrements(
            FunctionParameterRepository.convertBoolValue(v, "enable_position_increments")))
        .put("escape", (b, v) -> b.escape(
            FunctionParameterRepository.convertBoolValue(v, "escape")))
        .put("fuzziness", (b, v) -> b.fuzziness(FunctionParameterRepository.convertFuzziness(v)))
        .put("fuzzy_max_expansions", (b, v) -> b.fuzzyMaxExpansions(
            FunctionParameterRepository.convertIntValue(v, "fuzzy_max_expansions")))
        .put("fuzzy_prefix_length", (b, v) -> b.fuzzyPrefixLength(
            FunctionParameterRepository.convertIntValue(v, "fuzzy_prefix_length")))
        .put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(
            FunctionParameterRepository.checkRewrite(v, "fuzzy_rewrite")))
        .put("fuzzy_transpositions", (b, v) -> b.fuzzyTranspositions(
            FunctionParameterRepository.convertBoolValue(v, "fuzzy_transpositions")))
        .put("lenient", (b, v) -> b.lenient(
            FunctionParameterRepository.convertBoolValue(v, "lenient")))
        .put("max_determinized_states", (b, v) -> b.maxDeterminizedStates(
            FunctionParameterRepository.convertIntValue(v, "max_determinized_states")))
        .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
        .put("phrase_slop", (b, v) -> b.phraseSlop(
            FunctionParameterRepository.convertIntValue(v, "phrase_slop")))
        .put("quote_analyzer", (b, v) -> b.quoteAnalyzer(v.stringValue()))
        .put("quote_field_suffix", (b, v) -> b.quoteFieldSuffix(v.stringValue()))
        .put("rewrite", (b, v) -> b.rewrite(
            FunctionParameterRepository.checkRewrite(v, "rewrite")))
        .put("tie_breaker", (b, v) -> b.tieBreaker(
            FunctionParameterRepository.convertFloatValue(v, "tie_breaker")))
        .put("time_zone", (b, v) -> b.timeZone(FunctionParameterRepository.checkTimeZone(v)))
        .put("type", (b, v) -> b.type(FunctionParameterRepository.convertType(v)))
        .build());
  }


  /**
   * Builds QueryBuilder with query value and other default parameter values set.
   *
   * @param fields : A map of field names and their boost values
   * @param query : Query value for query_string query
   * @return : Builder for query_string query
   */
  @Override
  protected QueryStringQueryBuilder createBuilder(ImmutableMap<String, Float> fields,
                                                  String query) {
    return QueryBuilders.queryStringQuery(query).fields(fields);
  }

  @Override
  protected String getQueryName() {
    return QueryStringQueryBuilder.NAME;
  }
}
