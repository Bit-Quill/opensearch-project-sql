/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;

/**
 * Class for Lucene query that builds the query_string query.
 */
public class QueryQuery extends NoFieldQuery {
  /**
   * Default constructor for QueryQuery configures how RelevanceQuery.build() handles
   * named arguments by calling the constructor of QueryStringQuery.
   */
  public QueryQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<QueryStringQueryBuilder>>builder()
        .put("allow_leading_wildcard", (b, v) -> b.allowLeadingWildcard(
            FunctionParameterRepository.convertBoolValue(v, "allow_leading_wildcard")))
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("analyze_wildcard", (b, v) -> b.analyzeWildcard(
            FunctionParameterRepository.convertBoolValue(v, "analyze_wildcard")))
        .put("auto_generate_synonyms_phrase_query", (b, v) -> b.autoGenerateSynonymsPhraseQuery(
            FunctionParameterRepository.convertBoolValue(v, "auto_generate_synonyms_phrase_query")))
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
   * @param query : Query value for query_string query
   * @return : Builder for query query
   */
  protected QueryStringQueryBuilder createBuilder(String query) {
    return QueryBuilders.queryStringQuery(query);
  }

  @Override
  protected String getQueryName() {
    return QueryStringQueryBuilder.NAME;
  }
}
