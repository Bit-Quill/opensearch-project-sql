/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Initializes MatchQueryBuilder from a FunctionExpression.
 */
public class Match_QueryQuery extends SingleFieldQuery<MatchQueryBuilder> {

  private final String MATCH_QUERY_QUERY_NAME = "match_query";
  /**
   *  Default constructor for MatchQuery configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public Match_QueryQuery() {
    super(FunctionParameterRepository.MatchQueryBuildActions);
  }

  @Override
  protected MatchQueryBuilder createBuilder(String field, String query) {
    return QueryBuilders.matchQuery(field, query);
  }

  @Override
  protected String getQueryName() {
    return MATCH_QUERY_QUERY_NAME;
  }
}
