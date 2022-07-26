/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

public class MultiMatchQuery extends MultiFieldQuery<MultiMatchQueryBuilder> {
  /**
   *  Default constructor for MultiMatch configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public MultiMatchQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<MultiMatchQueryBuilder>>builder()
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("auto_generate_synonyms_phrase_query", (b, v) ->
            b.autoGenerateSynonymsPhraseQuery(FunctionParameterRepository
                .convertBoolValue(v, "auto_generate_synonyms_phrase_query")))
        .put("boost", (b, v) -> b.boost(
            FunctionParameterRepository.convertFloatValue(v, "boost")))
        .put("cutoff_frequency", (b, v) -> b.cutoffFrequency(
            FunctionParameterRepository.convertFloatValue(v, "cutoff_frequency")))
        .put("fuzziness", (b, v) -> b.fuzziness(FunctionParameterRepository.convertFuzziness(v)))
        .put("fuzzy_transpositions", (b, v) -> b.fuzzyTranspositions(
            FunctionParameterRepository.convertBoolValue(v, "fuzzy_transpositions")))
        .put("lenient", (b, v) -> b.lenient(
            FunctionParameterRepository.convertBoolValue(v, "lenient")))
        .put("max_expansions", (b, v) -> b.maxExpansions(
            FunctionParameterRepository.convertIntValue(v, "max_expansions")))
        .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
        .put("operator", (b, v) -> b.operator(
            FunctionParameterRepository.convertOperator(v, "operator")))
        .put("prefix_length", (b, v) -> b.prefixLength(
            FunctionParameterRepository.convertIntValue(v, "prefix_length")))
        .put("slop", (b, v) -> b.slop(
            FunctionParameterRepository.convertIntValue(v, "slop")))
        .put("tie_breaker", (b, v) -> b.tieBreaker(
            FunctionParameterRepository.convertFloatValue(v, "tie_breaker")))
        .put("type", (b, v) -> b.type(FunctionParameterRepository.convertType(v)))
        .put("zero_terms_query", (b, v) -> b.zeroTermsQuery(
            FunctionParameterRepository.convertZeroTermsQuery(v)))
        .build());
  }

  @Override
  protected MultiMatchQueryBuilder createBuilder(ImmutableMap<String, Float> fields, String query) {
    return QueryBuilders.multiMatchQuery(query).fields(fields);
  }

  @Override
  protected String getQueryName() {
    return MultiMatchQueryBuilder.NAME;
  }
}
