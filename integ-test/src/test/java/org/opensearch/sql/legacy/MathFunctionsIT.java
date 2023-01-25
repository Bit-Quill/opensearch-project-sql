/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONObject;
import org.junit.Test;

public class MathFunctionsIT extends SQLIntegTestCase {


  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void lowerCaseFunctionCall() {
    String query =
            String.format("SELECT abs(age - 100) AS abs FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("abs(age - 100)", "abs", "long"));
    assertEquals(68, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void upperCaseFunctionCall() {
    String query =
            String.format("SELECT ABS(age - 100) AS abs FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("ABS(age - 100)", "abs", "long"));
    assertEquals(68, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
  }

  @Test
  public void eulersNumber() {
    String query =
            String.format("SELECT E() AS e FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("E()", "e", "double"));
    assertEquals(Math.E,
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0);
  }

  @Test
  public void pi() {
    String query =
            String.format("SELECT PI() AS pi FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("PI()", "pi", "double"));
    assertEquals(Math.PI,
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0);
  }

  @Test
  public void expm1Function() {
    String query =
            String.format("SELECT EXPM1(2) AS expm1 FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
//    verifySchema(result, schema("EXPM1(2)", "expm1", "double"));
    assertEquals(Math.expm1(2),
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0);
  }

  @Test
  public void degreesFunction() {
    String query =
            String.format("SELECT age, DEGREES(age) AS degrees FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("DEGREES(age)", "degrees", "double"),
            schema("age", null, "long"));
    double age = result.getJSONArray("datarows").getJSONArray(0).getDouble(0);
    double functionResult = result.getJSONArray("datarows").getJSONArray(0).getDouble(1);
    assertEquals(Math.toDegrees(age), functionResult, 0);
  }

  @Test
  public void radiansFunction() {
    String query =
            String.format("SELECT age, RADIANS(age) as radians FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("RADIANS(age)", "radians", "double"),
            schema("age", null, "long"));
    double age = result.getJSONArray("datarows").getJSONArray(0).getDouble(0);
    double functionResult = result.getJSONArray("datarows").getJSONArray(0).getDouble(1);
    assertEquals(Math.toRadians(age), functionResult, 0);
  }

  @Test
  public void sin() {
    String query =
            String.format("SELECT SIN(PI()) as sin FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("SIN(PI())", "sin", "double"));
    assertEquals(Math.sin(Math.PI),
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0);
  }

  @Test
  public void asin() {
    String query =
            String.format("SELECT ASIN(PI()) as asin FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("ASIN(PI())", "asin", "double"));
  }

  @Test
  public void sinh() {
    String query =
            String.format("SELECT SINH(PI()) as sinh FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
//    verifySchema(result, schema("SINH(PI())", "sinh", "double"));
    assertEquals(Math.sinh(Math.PI),
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0);
  }

  @Test
  public void power() {
    String query =
            String.format("SELECT POWER(age, 2) AS power FROM %s " +
                    "WHERE (age IS NOT NULL) AND (balance IS NOT NULL) and (POWER(balance, 3) > 0)",
                    TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("POWER(age, 2)", "power", "double"));
    assertEquals(1024.0,
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0);
  }

  @Test
  public void atan2() {
    String query =
            String.format("SELECT ATAN2(age, age) AS atan2 FROM %s " +
                            "WHERE (age IS NOT NULL) AND (ATAN2(age, age) > 0)",
                    TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("ATAN2(age, age)", "atan2", "double"));
    assertEquals(Math.atan2(1, 1),
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0);
  }

  @Test
  public void cot() {
    String query =
            String.format("SELECT COT(PI()) AS cot FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("COT(PI())", "cot", "double"));
    assertEquals(1 / Math.tan(Math.PI),
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0.001);
  }

  @Test
  public void sign() {
    String query =
            String.format("SELECT SIGN(E()) AS sign FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("SIGN(E())", "sign", "integer"));
    assertEquals(Math.signum(Math.E),
            result.getJSONArray("datarows").getJSONArray(0).getInt(0), 0);
  }

  @Test
  public void logWithOneParam() {
    String query =
            String.format("SELECT LOG(3) AS log FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("LOG(3)", "log", "double"));
    assertEquals(Math.log(3),
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0);
  }

  @Test
  public void logWithTwoParams() {
    String query =
            String.format("SELECT LOG(2, 3) AS log FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("LOG(2, 3)", "log", "double"));
    assertEquals(Math.log(3) / Math.log(2),
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0.0001);
  }

  @Test
  public void logInAggregationShouldPass() {
    assertThat(
        executeQuery(
            "SELECT LOG(age) FROM " + TEST_INDEX_ACCOUNT
                + " WHERE age IS NOT NULL GROUP BY LOG(age) ORDER BY LOG(age)", "jdbc"
        ),
        containsString("\"type\": \"double\"")
    );
    assertThat(
        executeQuery(
            "SELECT LOG(2, age) FROM " + TEST_INDEX_ACCOUNT +
                " WHERE age IS NOT NULL GROUP BY LOG(2, age) ORDER BY LOG(2, age)", "jdbc"
        ),
        containsString("\"type\": \"double\"")
    );
  }

  @Test
  public void log10Test() {
    String query =
            String.format("SELECT log10(1000) AS log10 FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("log10(1000)", "log10", "double"));
    assertEquals(Math.log10(1000),
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0);
  }

  @Test
  public void ln() {
    String query =
            String.format("SELECT LN(5) AS ln FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("LN(5)", "ln", "double"));
    assertEquals(Math.log(5),
            result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0);
  }

  @Test
  public void lnInAggregationShouldPass() {
    assertThat(
        executeQuery(
            "SELECT LN(age) FROM " + TEST_INDEX_ACCOUNT +
                " WHERE age IS NOT NULL GROUP BY LN(age) ORDER BY LN(age)", "jdbc"
        ),
        containsString("\"type\": \"double\"")
    );
  }

  @Test
  public void rand() {
    String query =
            String.format("SELECT RAND() AS rand FROM %s", TEST_INDEX_ACCOUNT);
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1000, result.getInt("total"));
    verifySchema(result, schema("RAND()", "rand", "float"));
  }
}
