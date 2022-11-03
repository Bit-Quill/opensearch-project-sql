/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.WildcardQueryBuilder;

/**
 * Lucene query that builds wildcard query.
 */
public class WildcardQuery extends SingleFieldQuery<WildcardQueryBuilder> {
  /**
   *  Default constructor for WildcardQuery configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public WildcardQuery() {
    super(ImmutableMap.<String,
        QueryBuilderStep<WildcardQueryBuilder>>builder().build());
  }

  private String convertSqlWildcardToLucene(String text) {
    return text.replace('%', '*')
               .replace('_', '?');
  }

  @Override
  protected String getQueryName() {
    return WildcardQueryBuilder.NAME;
  }

  @Override
  protected WildcardQueryBuilder createBuilder(String field, String query) {
    String matchText = convertSqlWildcardToLucene(query);
    return QueryBuilders.wildcardQuery(field, matchText);
  }
}
