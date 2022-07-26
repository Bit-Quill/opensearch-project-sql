/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilders;

/**
 * Initializes MatchBoolPrefixQueryBuilder from a FunctionExpression.
 */
public class MatchBoolPrefixQuery
    extends SingleFieldQuery<MatchBoolPrefixQueryBuilder> {
  /**
   * Constructor for MatchBoolPrefixQuery to configure RelevanceQuery
   * with support of optional parameters.
   */
  public MatchBoolPrefixQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<MatchBoolPrefixQueryBuilder>>builder()
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("boost", (b, v) -> b.boost(
            FunctionParameterRepository.convertFloatValue(v, "boost")))
        .put("fuzziness", (b, v) -> b.fuzziness(FunctionParameterRepository.convertFuzziness(v)))
        .put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(
            FunctionParameterRepository.checkRewrite(v, "fuzzy_rewrite")))
        .put("fuzzy_transpositions", (b, v) -> b.fuzzyTranspositions(
            FunctionParameterRepository.convertBoolValue(v, "fuzzy_transpositions")))
        .put("max_expansions", (b, v) -> b.maxExpansions(
            FunctionParameterRepository.convertIntValue(v, "max_expansions")))
        .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
        .put("operator", (b, v) -> b.operator(
            FunctionParameterRepository.convertOperator(v, "operator")))
        .put("prefix_length", (b, v) -> b.prefixLength(
            FunctionParameterRepository.convertIntValue(v, "prefix_length")))
        .build());
  }

  /**
   * Maps correct query builder function to class.
   * @param field  Field to execute query in
   * @param query  Text used to search field
   * @return  Object of executed query
   */
  @Override
  protected MatchBoolPrefixQueryBuilder createBuilder(String field, String query) {
    return QueryBuilders.matchBoolPrefixQuery(field, query);
  }

  @Override
  protected String getQueryName() {
    return MatchBoolPrefixQueryBuilder.NAME;
  }
}
