/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

public class ScoreQueryIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  /**
   * "query" : {
   *   "from": 0,
   *   "size": 3,
   *   "timeout": "1m",
   *   "query": {
   *     "bool": {
   *       "should": [
   *         {
   *           "match": {
   *             "address": {
   *               "query": "Lane",
   *               "operator": "OR",
   *               "prefix_length": 0,
   *               "max_expansions": 50,
   *               "fuzzy_transpositions": true,
   *               "lenient": false,
   *               "zero_terms_query": "NONE",
   *               "auto_generate_synonyms_phrase_query": true,
   *               "boost": 100.0
   *             }
   *           }
   *         },
   *         {
   *           "match": {
   *             "address": {
   *               "query": "Street",
   *               "operator": "OR",
   *               "prefix_length": 0,
   *               "max_expansions": 50,
   *               "fuzzy_transpositions": true,
   *               "lenient": false,
   *               "zero_terms_query": "NONE",
   *               "auto_generate_synonyms_phrase_query": true,
   *               "boost": 0.5
   *             }
   *           }
   *         }
   *       ],
   *       "adjust_pure_negative": true,
   *       "boost": 1.0
   *     }
   *   },
   *   "_source": {
   *     "includes": [
   *       "address"
   *     ],
   *     "excludes": []
   *   },
   *   "sort": [
   *     {
   *       "_score": {
   *         "order": "desc"
   *       }
   *     }
   *   ],
   *   "track_scores": true
   * }
   * @throws IOException
   */
  @Test
  public void scoreQueryTest() throws IOException {
    final String result = explainQuery(String.format(Locale.ROOT,
        "select address from %s " +
            "where score(matchQuery(address, 'Lane'),100) " +
            "or score(matchQuery(address,'Street'),0.5) order by _score desc limit 3",
        TestsConstants.TEST_INDEX_ACCOUNT));
    Assert.assertThat(result, containsString("\\\"match\\\":{\\\"address\\\":{\\\"query\\\":\\\"Lane\\\""));
    Assert.assertThat(result, containsString("\\\"boost\\\":100.0"));
    Assert.assertThat(result, containsString("\\\"match\\\":{\\\"address\\\":{\\\"query\\\":\\\"Street\\\""));
    Assert.assertThat(result, containsString("\\\"boost\\\":0.5"));
    Assert.assertThat(result, containsString("\\\"sort\\\":[{\\\"_score\\\""));
    Assert.assertThat(result, containsString("\\\"track_scores\\\":true"));
  }

  @Test
  public void scoreQueryDefaultBoostTest() throws IOException {
    final String result = explainQuery(String.format(Locale.ROOT,
        "select address from %s " +
            "where score(matchQuery(address, 'Lane')) order by _score desc limit 2",
        TestsConstants.TEST_INDEX_ACCOUNT));
    Assert.assertThat(result, containsString("\\\"match\\\":{\\\"address\\\":{\\\"query\\\":\\\"Lane\\\""));
    Assert.assertThat(result, containsString("\\\"boost\\\":1.0"));
    Assert.assertThat(result, containsString("\\\"sort\\\":[{\\\"_score\\\""));
    Assert.assertThat(result, containsString("\\\"track_scores\\\":true"));
  }
}
