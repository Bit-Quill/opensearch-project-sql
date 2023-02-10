/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PEOPLE2;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class JsonResponseTypeIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.PEOPLE2);
    loadIndex(Index.DOG);
    loadIndex(Index.CALCS);
  }

  // TODO: Verify JSON type response when using String and Date Functions and add test cases
  // For example: 'select concat(str1, '!!!!!!!!') from calcs' might not be handled as expected.
  // Also, handling of the date/time functions (for example: 'select now() from calcs) will need attention as well.

  @Test
  public void selectTest() {
    String responseAsString =
            executeQuery(String.format("SELECT * FROM %s", TEST_INDEX_PEOPLE2), "json");
    JSONObject response = new JSONObject(responseAsString);
    Assert.assertTrue(response.has("hits"));
    Assert.assertTrue(response.has("_shards"));
    Assert.assertTrue(response.has("took"));
    Assert.assertTrue(response.has("timed_out"));
    Assert.assertEquals(12, getTotalHits(response));
  }

  @Test
  public void aggregationTest() {
    final String aggregationsObjName = "aggregations";
    String responseAsString =
            executeQuery(String.format("SELECT count(*), sum(age) FROM %s", TEST_INDEX_PEOPLE2), "json");
    JSONObject response = new JSONObject(responseAsString);
    Assert.assertTrue(response.has(aggregationsObjName));
    Assert.assertEquals(12,
            response.getJSONObject(aggregationsObjName).getJSONObject("count(*)").getInt("value"));
    Assert.assertEquals(400,
            response.getJSONObject(aggregationsObjName).getJSONObject("sum(age)").getInt("value"));
  }

  @Test
  public void groupByTest() {
    String responseAsString =
            executeQuery(String.format("SELECT count(*) FROM %s group by age", TEST_INDEX_DOG), "json");
    Assert.assertTrue(responseAsString.contains("\"aggregations\":{\"composite_buckets\":{\"after_key\":{\"age\":4},"
            + "\"buckets\":[{\"key\":{\"age\":2},\"doc_count\":1,\"count(*)\":{\"value\":1}},{\"key\":{\"age\":4},"
            + "\"doc_count\":1,\"count(*)\":{\"value\":1}}]}}"));
  }

  @Test
  public void selectWithoutWhereTest() {
    String responseAsString =
            executeQuery(String.format("SELECT count(*) FROM %s group by age", TEST_INDEX_DOG), "json");

    Assert.assertTrue(responseAsString.contains("\"aggregations\":{\"composite_buckets\":{\"after_key\":{\"age\":4},"
            + "\"buckets\":[{\"key\":{\"age\":2},\"doc_count\":1,\"count(*)\":{\"value\":1}},{\"key\":{\"age\":4},"
            + "\"doc_count\":1,\"count(*)\":{\"value\":1}}]}}"));
  }

  @Test
  public void selectFromTwoDocumentsTest() {
    // This query will fall back to V1, as V2 doesn't support Joins yet.
    String responseAsString =
            executeQuery(String.format("SELECT d.age, c.int0 FROM %s c JOIN %s d WHERE c.int1 > 2",
                    TEST_INDEX_CALCS, TEST_INDEX_DOG), "json");

    JSONObject response = new JSONObject(responseAsString);
    Assert.assertTrue(response.has("hits"));

    JSONArray hits = response.getJSONObject("hits").getJSONArray("hits");
    Assert.assertTrue(hits.length() > 0);
    for (int i = 0; i < hits.length(); i++) {
      JSONObject jsonObject = hits.getJSONObject(i);
      Assert.assertTrue(jsonObject.getJSONObject("_source").keySet().contains("d.age"));
      Assert.assertTrue(jsonObject.getJSONObject("_source").keySet().contains("c.int0"));
    }
  }
 }

