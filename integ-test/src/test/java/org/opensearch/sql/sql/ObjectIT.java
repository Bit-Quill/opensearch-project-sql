package org.opensearch.sql.sql;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

import java.io.IOException;
import java.util.List;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_MULTI_NESTED_OBJECT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_OBJECT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

public class ObjectIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.NESTED_OBJECT);
    loadIndex(Index.MULTI_NESTED_OBJECT);
  }

  @Test
  public void object_array_of_objects_test() {
    String query = "SELECT message.info, comment.data FROM " + TEST_INDEX_NESTED_OBJECT;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(5, result.getInt("total"));
    verifyDataRows(result,
        rows("a", "ab"),
        rows("b", "aa"),
        rows("c", "aa"),
        rows(new JSONArray(List.of("c","a")), "ab"),
        rows(new JSONArray(List.of("zz")), new JSONArray(List.of("aa", "bb"))));
  }

  @Test
  public void object_with_array_of_multi_nested_objects_test() {
    String query = "SELECT message.author.name, message.info FROM " + TEST_INDEX_MULTI_NESTED_OBJECT;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(5, result.getInt("total"));
    verifyDataRows(result,
        rows("e", "a"),
        rows("f", "b"),
        rows("g", "c"),
        rows(new JSONArray(List.of("h", "p")), new JSONArray(List.of("d","i"))),
        rows(new JSONArray(List.of("yy")), new JSONArray(List.of("zz"))));
  }
}
