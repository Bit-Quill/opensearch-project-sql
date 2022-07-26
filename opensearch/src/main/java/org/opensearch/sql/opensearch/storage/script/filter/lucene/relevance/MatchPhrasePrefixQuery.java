/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
    * Lucene query that builds a match_phrase_prefix query.
    */
public class MatchPhrasePrefixQuery  extends SingleFieldQuery<MatchPhrasePrefixQueryBuilder> {
  /**
   *  Default constructor for MatchPhrasePrefixQuery configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public MatchPhrasePrefixQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<MatchPhrasePrefixQueryBuilder>>builder()
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("boost", (b, v) -> b.boost(
            FunctionParameterRepository.convertFloatValue(v, "boost")))
        .put("max_expansions", (b, v) -> b.maxExpansions(
            FunctionParameterRepository.convertIntValue(v, "max_expansions")))
        .put("slop", (b, v) -> b.slop(
            FunctionParameterRepository.convertIntValue(v, "slop")))
        .put("zero_terms_query", (b, v) -> b.zeroTermsQuery(
            FunctionParameterRepository.convertZeroTermsQuery(v)))
        .build());
  }

  @Override
  protected MatchPhrasePrefixQueryBuilder createBuilder(String field, String query) {
    return QueryBuilders.matchPhrasePrefixQuery(field, query);
  }

  @Override
  protected String getQueryName() {
    return MatchPhrasePrefixQueryBuilder.NAME;
  }
}
