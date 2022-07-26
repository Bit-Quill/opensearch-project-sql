/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilders;

/**
 * Initializes MatchQueryBuilder from a FunctionExpression.
 */
public class MatchQuery extends SingleFieldQuery<MatchQueryBuilder> {
  /**
   *  Default constructor for MatchQuery configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public MatchQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<MatchQueryBuilder>>builder()
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("auto_generate_synonyms_phrase_query", (b, v) -> b.autoGenerateSynonymsPhraseQuery(
            FunctionParameterRepository.convertBoolValue(v,"auto_generate_synonyms_phrase_query")))
        .put("boost", (b, v) -> b.boost(
            FunctionParameterRepository.convertFloatValue(v, "boost")))
        .put("fuzziness", (b, v) -> b.fuzziness(FunctionParameterRepository.convertFuzziness(v)))
        .put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(
            FunctionParameterRepository.checkRewrite(v, "fuzzy_rewrite")))
        .put("fuzzy_transpositions", (b, v) -> b.fuzzyTranspositions(
            FunctionParameterRepository.convertBoolValue(v, "fuzzy_transpositions")))
        .put("lenient", (b, v) -> b.lenient(
            FunctionParameterRepository.convertBoolValue(v, "lenient")))
        .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
        .put("max_expansions", (b, v) -> b.maxExpansions(
            FunctionParameterRepository.convertIntValue(v, "max_expansions")))
        .put("operator", (b, v) -> b.operator(
            FunctionParameterRepository.convertOperator(v, "operator")))
        .put("prefix_length", (b, v) -> b.prefixLength(
            FunctionParameterRepository.convertIntValue(v, "prefix_length")))
        .put("zero_terms_query", (b, v) -> b.zeroTermsQuery(
            FunctionParameterRepository.convertZeroTermsQuery(v)))
        .build());
  }

  @Override
  protected MatchQueryBuilder createBuilder(String field, String query) {
    return QueryBuilders.matchQuery(field, query);
  }

  @Override
  protected String getQueryName() {
    return MatchQueryBuilder.NAME;
  }
}
