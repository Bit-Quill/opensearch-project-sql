/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DDOUBLE;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class MathematicalFunctionIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.DDOUBLE);
  }

  @Test
  public void testPI() throws IOException {
    JSONObject result =
            executeQuery(String.format("SELECT PI() FROM %s HAVING (COUNT(1) > 0)",TEST_INDEX_BANK) );
    verifySchema(result,
            schema("PI()", null, "double"));
    verifyDataRows(result, rows(3.141592653589793));
  }

  @Test
  public void testConv() throws IOException {
    JSONObject result = executeQuery("select conv(11, 10, 16)");
    verifySchema(result, schema("conv(11, 10, 16)", null, "keyword"));
    verifyDataRows(result, rows("b"));

    result = executeQuery("select conv(11, 16, 10)");
    verifySchema(result, schema("conv(11, 16, 10)", null, "keyword"));
    verifyDataRows(result, rows("17"));
  }

  @Test
  public void testCrc32() throws IOException {
    JSONObject result = executeQuery("select crc32('MySQL')");
    verifySchema(result, schema("crc32('MySQL')", null, "long"));
    verifyDataRows(result, rows(3259397556L));
  }

  @Test
  public void testE() throws IOException {
    JSONObject result = executeQuery("select e()");
    verifySchema(result, schema("e()", null, "double"));
    verifyDataRows(result, rows(Math.E));
  }

  @Test
  public void testMod() throws IOException {
    JSONObject result = executeQuery("select mod(3, 2)");
    verifySchema(result, schema("mod(3, 2)", null, "integer"));
    verifyDataRows(result, rows(1));

    result = executeQuery("select mod(3.1, 2)");
    verifySchema(result, schema("mod(3.1, 2)", null, "double"));
    verifyDataRows(result, rows(1.1));
  }

  @Test
  public void testRound() throws IOException {
    JSONObject result = executeQuery("select round(56.78)");
    verifySchema(result, schema("round(56.78)", null, "double"));
    verifyDataRows(result, rows(57.0));

    result = executeQuery("select round(56.78, 1)");
    verifySchema(result, schema("round(56.78, 1)", null, "double"));
    verifyDataRows(result, rows(56.8));

    result = executeQuery("select round(56.78, -1)");
    verifySchema(result, schema("round(56.78, -1)", null, "double"));
    verifyDataRows(result, rows(60.0));

    result = executeQuery("select round(-56)");
    verifySchema(result, schema("round(-56)", null, "long"));
    verifyDataRows(result, rows(-56));

    result = executeQuery("select round(-56, 1)");
    verifySchema(result, schema("round(-56, 1)", null, "long"));
    verifyDataRows(result, rows(-56));

    result = executeQuery("select round(-56, -1)");
    verifySchema(result, schema("round(-56, -1)", null, "long"));
    verifyDataRows(result, rows(-60));

    result = executeQuery("select round(3.5)");
    verifySchema(result, schema("round(3.5)", null, "double"));
    verifyDataRows(result, rows(4.0));

    result = executeQuery("select round(-3.5)");
    verifySchema(result, schema("round(-3.5)", null, "double"));
    verifyDataRows(result, rows(-4.0));
  }

  /**
   * Test sign function with double value.
   */
  @Test
  public void testSign() throws IOException {
    JSONObject result = executeQuery("select sign(1.1)");
    verifySchema(result, schema("sign(1.1)", null, "integer"));
    verifyDataRows(result, rows(1));

    result = executeQuery("select sign(-1.1)");
    verifySchema(result, schema("sign(-1.1)", null, "integer"));
    verifyDataRows(result, rows(-1));
  }

  @Test
  public void testTruncate() throws IOException {
    JSONObject result = executeQuery("select truncate(56.78, 1)");
    verifySchema(result, schema("truncate(56.78, 1)", null, "double"));
    verifyDataRows(result, rows(56.7));

    result = executeQuery("select truncate(56.78, -1)");
    verifySchema(result, schema("truncate(56.78, -1)", null, "double"));
    verifyDataRows(result, rows(50.0));

    result = executeQuery("select truncate(-56, 1)");
    verifySchema(result, schema("truncate(-56, 1)", null, "long"));
    verifyDataRows(result, rows(-56));

    result = executeQuery("select truncate(-56, -1)");
    verifySchema(result, schema("truncate(-56, -1)", null, "long"));
    verifyDataRows(result, rows(-50));

    result = executeQuery("select truncate(33.33344, -1)");
    verifySchema(result, schema("truncate(33.33344, -1)", null, "double"));
    verifyDataRows(result, rows(30.0));

    result = executeQuery("select truncate(33.33344, 2)");
    verifySchema(result, schema("truncate(33.33344, 2)", null, "double"));
    verifyDataRows(result, rows(33.33));

    result = executeQuery("select truncate(33.33344, 100)");
    verifySchema(result, schema("truncate(33.33344, 100)", null, "double"));
    verifyDataRows(result, rows(33.33344));

    result = executeQuery("select truncate(33.33344, 0)");
    verifySchema(result, schema("truncate(33.33344, 0)", null, "double"));
    verifyDataRows(result, rows(33.0));

    result = executeQuery("select truncate(33.33344, 4)");
    verifySchema(result, schema("truncate(33.33344, 4)", null, "double"));
    verifyDataRows(result, rows(33.3334));

    result = executeQuery(String.format("select truncate(%s, 6)", Math.PI));
    verifySchema(result, schema(String.format("truncate(%s, 6)", Math.PI), null, "double"));
    verifyDataRows(result, rows(3.141592));

    String query = "select val, truncate(val, 1) from %s";
    JSONObject response = executeJdbcRequest(String.format(query, TEST_INDEX_DDOUBLE));
    verifySchema(response, schema("val", null, "double"),
            schema("truncate(val, 1)", null, "double"));
    assertEquals(20, response.getInt("total"));

    verifyDataRows(response,
            rows(null, null), rows(-9.223372036854776e+18, -9.223372036854776e+18),
            rows(-2147483649.0, -2147483649.0), rows(-2147483648.0, -2147483648.0),
            rows(-32769.0, -32769.0), rows(-32768.0, -32768.0),
            rows(-34.84, -34.8), rows(-2.0, -2.0),
            rows(-1.2, -1.2), rows(-1.0, -1.0),
            rows(0.0 , 0.0), rows(1.0, 1.0),
            rows(1.3, 1.3), rows(2.0, 2.0),
            rows(1004.3, 1004.3), rows(32767.0 , 32767.0 ),
            rows(32768.0 , 32768.0 ), rows(2147483647.0, 2147483647.0),
            rows(2147483648.0, 2147483648.0),
            rows(9.223372036854776e+18, 9.223372036854776e+18));
  }

  @Test
  public void testAtan() throws IOException {
    JSONObject result = executeQuery("select atan(2, 3)");
    verifySchema(result, schema("atan(2, 3)", null, "double"));
    verifyDataRows(result, rows(Math.atan2(2, 3)));
  }

  protected JSONObject executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }


  @Test
  public void testCbrt() throws IOException {
    JSONObject result = executeQuery("select cbrt(8)");
    verifySchema(result, schema("cbrt(8)", "double"));
    verifyDataRows(result, rows(2.0));

    result = executeQuery("select cbrt(9.261)");
    verifySchema(result, schema("cbrt(9.261)", "double"));
    verifyDataRows(result, rows(2.1));

    result = executeQuery("select cbrt(-27)");
    verifySchema(result, schema("cbrt(-27)", "double"));
    verifyDataRows(result, rows(-3.0));
  }
}
