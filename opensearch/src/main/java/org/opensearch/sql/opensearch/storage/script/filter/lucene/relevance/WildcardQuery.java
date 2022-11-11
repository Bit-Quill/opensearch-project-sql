/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

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
    super(FunctionParameterRepository.WildcardQueryBuildActions);
  }

  private static final char DEFAULT_ESCAPE = '\\';

  public String convertSqlWildcardToLucene(String text) {
    StringBuilder convertedString = new StringBuilder(text.length());
    boolean escaped = false;

    for (char currentChar : text.toCharArray()) {
      switch (currentChar) {
        case DEFAULT_ESCAPE:
          escaped = true;
          convertedString.append(currentChar);
          break;
        case '%':
          if (escaped) {
            convertedString.deleteCharAt(convertedString.length() - 1);
            convertedString.append("%");
          } else {
            convertedString.append("*");
          }
          escaped = false;
          break;
        case '_':
          if (escaped) {
            convertedString.deleteCharAt(convertedString.length() - 1);
            convertedString.append("_");
          } else {
            convertedString.append('?');
          }
          escaped = false;
          break;
        default:
          convertedString.append(currentChar);
          escaped = false;
      }
    }
    return convertedString.toString();
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
