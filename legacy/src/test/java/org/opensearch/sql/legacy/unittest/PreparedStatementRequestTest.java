/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest;

import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.request.PreparedStatementRequest;

public class PreparedStatementRequestTest {

  @Test
  public void testSubstitute() {
    String sqlTemplate =
        "select * from table_name where number_param > ? and string_param = 'Amazon.com' "
            + "and test_str = '''test escape? \\'' and state in (?,?) and null_param = ? and double_param = ? "
            + "and question_mark = '?'";
    List<PreparedStatementRequest.PreparedStatementParameter> params = new ArrayList<>();
    params.add(new PreparedStatementRequest.PreparedStatementParameter<Integer>(10));
    params.add(new PreparedStatementRequest.StringParameter("WA"));
    params.add(new PreparedStatementRequest.StringParameter(""));
    params.add(new PreparedStatementRequest.NullParameter());
    params.add(new PreparedStatementRequest.PreparedStatementParameter<Double>(2.0));
    PreparedStatementRequest psr =
        new PreparedStatementRequest(sqlTemplate, new JSONObject(), params);
    String generatedSql = psr.getSql();

    String expectedSql =
        "select * from table_name where number_param > 10 and string_param = 'Amazon.com' "
            + "and test_str = '''test escape? \\'' and state in ('WA','') and null_param = null "
            + "and double_param = 2.0 and question_mark = '?'";
    Assert.assertEquals(expectedSql, generatedSql);
  }

  @Test
  public void testStringParameter() {
    PreparedStatementRequest.StringParameter param;
    param = new PreparedStatementRequest.StringParameter("test string");
    Assert.assertEquals("'test string'", param.getSqlSubstitutionValue());

    param = new PreparedStatementRequest.StringParameter("test ' single ' quote '");
    Assert.assertEquals("'test \\' single \\' quote \\''", param.getSqlSubstitutionValue());

    param = new PreparedStatementRequest.StringParameter("test line \n break \n char");
    Assert.assertEquals("'test line \\n break \\n char'", param.getSqlSubstitutionValue());

    param = new PreparedStatementRequest.StringParameter("test carriage \r return \r char");
    Assert.assertEquals("'test carriage \\r return \\r char'", param.getSqlSubstitutionValue());

    param = new PreparedStatementRequest.StringParameter("test \\ backslash \\ char");
    Assert.assertEquals("'test \\\\ backslash \\\\ char'", param.getSqlSubstitutionValue());

    param = new PreparedStatementRequest.StringParameter("test single ' quote ' char");
    Assert.assertEquals("'test single \\' quote \\' char'", param.getSqlSubstitutionValue());

    param = new PreparedStatementRequest.StringParameter("test double \" quote \" char");
    Assert.assertEquals("'test double \\\" quote \\\" char'", param.getSqlSubstitutionValue());
  }

  @Test(expected = IllegalStateException.class)
  public void testSubstitute_parameterNumberNotMatch() {
    String sqlTemplate = "select * from table_name where param1 = ? and param2 = ?";
    List<PreparedStatementRequest.PreparedStatementParameter> params = new ArrayList<>();
    params.add(new PreparedStatementRequest.StringParameter("value"));

    PreparedStatementRequest psr =
        new PreparedStatementRequest(sqlTemplate, new JSONObject(), params);
  }

  @Test
  public void testSubstitute_nullSql() {
    PreparedStatementRequest psr = new PreparedStatementRequest(null, new JSONObject(), null);

    Assert.assertNull(psr.getSql());
  }
}
