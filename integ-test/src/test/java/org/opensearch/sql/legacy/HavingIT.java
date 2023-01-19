/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class HavingIT extends SQLIntegTestCase {

  private static final String SELECT_FROM_WHERE_GROUP_BY =
      "SELECT state, COUNT(*) cnt " +
          "FROM " + TestsConstants.TEST_INDEX_ACCOUNT + " " +
          "WHERE age = 30 " +
          "GROUP BY state ";

  private static final Map<Integer, List<String>> states1 = expectedData(1, Arrays.asList(
      "AK", "AR", "CT", "DE", "HI", "IA", "IL", "IN", "LA", "MA", "MD", "MN",
      "MO", "MT", "NC", "ND", "NE", "NH", "NJ", "NV", "SD", "VT", "WV", "WY"
  ));
  private static final Map<Integer, List<String>> states2 =
          expectedData(2, Arrays.asList("AZ", "DC", "KS", "ME"));
  private static final Map<Integer, List<String>> states3 =
        expectedData(3, Arrays.asList("AL", "ID", "KY", "OR", "TN"));

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void equalsTo() {
    assertEquals(
        query(SELECT_FROM_WHERE_GROUP_BY + "HAVING cnt = 2"),
            states2
    );
  }

  @Test
  public void lessThanOrEqual() {
    assertEquals(
        query(SELECT_FROM_WHERE_GROUP_BY + "HAVING cnt <= 2"),
            mergeData(states1, states2)
    );
  }

  @Test
  public void notEqualsTo() {
    assertEquals(
        query(SELECT_FROM_WHERE_GROUP_BY + "HAVING cnt <> 2"),
            mergeData(states1, states3)
    );
  }

  @Test
  public void between() {
    assertEquals(
        query(SELECT_FROM_WHERE_GROUP_BY + "HAVING cnt BETWEEN 1 AND 2"),
            mergeData(states1, states2)
    );
  }

  @Test
  public void notBetween() {
    assertEquals(
        query(SELECT_FROM_WHERE_GROUP_BY + "HAVING cnt NOT BETWEEN 1 AND 2"),
            states3
    );
  }

  @Test
  public void in() {
    assertEquals(
        query(SELECT_FROM_WHERE_GROUP_BY + "HAVING cnt IN (2, 3)"),
            mergeData(states3, states2)
    );
  }

  @Test
  public void notIn() {
    assertEquals(
        query(SELECT_FROM_WHERE_GROUP_BY + "HAVING cnt NOT IN (2, 3)"),
            states1
    );
  }

  @Test
  public void and() {
    assertEquals(
        query(SELECT_FROM_WHERE_GROUP_BY + "HAVING cnt >= 1 AND cnt < 3"),
            mergeData(states1, states2)
    );
  }

  @Test
  public void or() {
    assertEquals(
        query(SELECT_FROM_WHERE_GROUP_BY + "HAVING cnt = 1 OR cnt = 3"),
            mergeData(states1, states3)
    );
  }

  @Test
  public void not() {
    assertEquals(
        query(SELECT_FROM_WHERE_GROUP_BY + "HAVING NOT cnt >= 2"), 
            states1
    );
  }

  @Test
  public void notAndOr() {
    assertEquals(
        query(SELECT_FROM_WHERE_GROUP_BY + "HAVING NOT (cnt > 0 AND cnt <= 2)"),
            states3
    );
  }

  private Map<Integer, List<String>> query(String query) {
    JSONObject response = executeJdbcRequest(query);
    return getResult(response);
  }

  private Map<Integer, List<String>> getResult(JSONObject response) {
    Map<Integer, List<String>> result = new HashMap<>();
    JSONArray datarows = response.getJSONArray("datarows");
    int key = 0;
    List<String> values = new ArrayList<>();
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray hit = datarows.getJSONArray(i);
      values.add(hit.getString(0));
      key = hit.getInt(1);
    }
    result.put(key, values);

    return result;
  }

  private static Map<Integer, List<String>> expectedData(Integer count, List<String> states) {
    Map<Integer, List<String>> result = new HashMap<>();
    result.put(count, states);
    return result;
  }

  private static Map<Integer, List<String>> mergeData(Map<Integer, List<String>> data1,
                                                      Map<Integer, List<String>> data2) {
    Map<Integer, List<String>> result = new HashMap<>();
    List<String> data1Values = data1.entrySet().stream().findFirst().get().getValue();
    List<String> data2Values = data2.entrySet().stream().findFirst().get().getValue();
    int dat1Key = data1.keySet().stream().findFirst().get();
    List<String> mergedData = Stream.concat(data1Values.stream(), data2Values.stream())
            .sorted().collect(Collectors.toList());
    result.put(dat1Key, mergedData);
    return result;
  }
}
