/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class ArithmeticFunctionIT extends SQLIntegTestCase {

    @Override
    public void init() throws Exception {
        super.init();
        loadIndex(Index.BANK);
    }

    @Test
    public void testAdd() throws IOException {
        JSONObject result = executeQuery("select add(3, 2)");
        verifySchema(result, schema("add(3, 2)", null, "integer"));
        verifyDataRows(result, rows(3 * 2));

        result = executeQuery("select add(2.5, 2)");
        verifySchema(result, schema("add(2.5, 2)", null, "double"));
        verifyDataRows(result, rows(2.5D * 2));

        result = executeQuery("select add(3000000000, 2)");
        verifySchema(result, schema("add(3000000000, 2)", null, "long"));
        verifyDataRows(result, rows(3000000000L * 2));

        result = executeQuery("select add(CAST(1.6 AS FLOAT), 2)");
        verifySchema(result, schema("add(CAST(1.6 AS FLOAT), 2)", null, "float"));
        verifyDataRows(result, rows(1.6 * 2));
    }

    public void testAddFunction() throws IOException {
        JSONObject result = executeQuery("select 3 + 2");
        verifySchema(result, schema("3 + 2", null, "integer"));
        verifyDataRows(result, rows(3 + 2));

        result = executeQuery("select 2.5 + 2");
        verifySchema(result, schema("2.5 + 2", null, "double"));
        verifyDataRows(result, rows(2.5D + 2));

        result = executeQuery("select 3000000000 + 2");
        verifySchema(result, schema("3000000000 + 2", null, "long"));
        verifyDataRows(result, rows(3000000000L + 2));

        result = executeQuery("select CAST(1.6 AS FLOAT) + 2");
        verifySchema(result, schema("CAST(1.6 AS FLOAT) + 2", null, "float"));
        verifyDataRows(result, rows(1.6 + 2));
    }

    public void testDivide() throws IOException {
        JSONObject result = executeQuery("select divide(3, 2)");
        verifySchema(result, schema("divide(3, 2)", null, "integer"));
        verifyDataRows(result, rows(3 / 2));

        result = executeQuery("select divide(2.5, 2)");
        verifySchema(result, schema("divide(2.5, 2)", null, "double"));
        verifyDataRows(result, rows(2.5D / 2));

        result = executeQuery("select divide(3000000000, 2)");
        verifySchema(result, schema("divide(3000000000, 2)", null, "long"));
        verifyDataRows(result, rows(3000000000L / 2));

        result = executeQuery("select divide(CAST(1.6 AS FLOAT), 2)");
        verifySchema(result, schema("divide(CAST(1.6 AS FLOAT), 2)", null, "float"));
        verifyDataRows(result, rows(1.6 / 2));
    }

    public void testDivideFunction() throws IOException {
        JSONObject result = executeQuery("select 3 / 2");
        verifySchema(result, schema("3 / 2", null, "integer"));
        verifyDataRows(result, rows(3 / 2));

        result = executeQuery("select 2.5 / 2");
        verifySchema(result, schema("2.5 / 2", null, "double"));
        verifyDataRows(result, rows(2.5D / 2));

        result = executeQuery("select 3000000000 / 2");
        verifySchema(result, schema("3000000000 / 2", null, "long"));
        verifyDataRows(result, rows(3000000000L / 2));

        result = executeQuery("select CAST(1.6 AS FLOAT) / 2");
        verifySchema(result, schema("CAST(1.6 AS FLOAT) / 2", null, "float"));
        verifyDataRows(result, rows(1.6 / 2));
    }

    public void testMod() throws IOException {
        JSONObject result = executeQuery("select mod(3, 2)");
        verifySchema(result, schema("mod(3, 2)", null, "integer"));
        verifyDataRows(result, rows(3 % 2));

        result = executeQuery("select mod(2.5, 2)");
        verifySchema(result, schema("mod(2.5, 2)", null, "double"));
        verifyDataRows(result, rows(2.5D % 2));

        result = executeQuery("select mod(3000000000, 2)");
        verifySchema(result, schema("mod(3000000000, 2)", null, "long"));
        verifyDataRows(result, rows(3000000000L % 2));

        result = executeQuery("select mod(CAST(1.6 AS FLOAT), 2)");
        verifySchema(result, schema("mod(CAST(1.6 AS FLOAT), 2)", null, "float"));
        verifyDataRows(result, rows(1.6 % 2));
    }

    public void testModulus() throws IOException {
        JSONObject result = executeQuery("select 3 % 2");
        verifySchema(result, schema("3 % 2", null, "integer"));
        verifyDataRows(result, rows(3 % 2));

        result = executeQuery("select 2.5 % 2");
        verifySchema(result, schema("select 2.5 % 2", null, "double"));
        verifyDataRows(result, rows(2.5D % 2));

        result = executeQuery("select 3000000000 % 2");
        verifySchema(result, schema("3000000000 % 2", null, "long"));
        verifyDataRows(result, rows(3000000000L % 2));

        result = executeQuery("select CAST(1.6 AS FLOAT) % 2");
        verifySchema(result, schema("CAST(1.6 AS FLOAT) % 2", null, "float"));
        verifyDataRows(result, rows(1.6 % 2));
    }

    public void testModulusFunction() throws IOException {
        JSONObject result = executeQuery("select modulus(3, 2)");
        verifySchema(result, schema("modulus(3, 2)", null, "integer"));
        verifyDataRows(result, rows(3 % 2));

        result = executeQuery("select modulus(2.5, 2)");
        verifySchema(result, schema("modulus(2.5, 2)", null, "double"));
        verifyDataRows(result, rows(2.5D % 2));

        result = executeQuery("select modulus(3000000000, 2)");
        verifySchema(result, schema("modulus(3000000000, 2)", null, "long"));
        verifyDataRows(result, rows(3000000000L % 2));

        result = executeQuery("select modulus(CAST(1.6 AS FLOAT), 2)");
        verifySchema(result, schema("modulus(CAST(1.6 AS FLOAT), 2)", null, "float"));
        verifyDataRows(result, rows(1.6 % 2));
    }

    public void testMultiply() throws IOException {
        JSONObject result = executeQuery("select 3 * 2");
        verifySchema(result, schema("3 * 2", null, "integer"));
        verifyDataRows(result, rows(3 * 2));

        result = executeQuery("select 2.5 * 2");
        verifySchema(result, schema("2.5 * 2", null, "double"));
        verifyDataRows(result, rows(2.5D * 2));

        result = executeQuery("select 3000000000 * 2");
        verifySchema(result, schema("3000000000 * 2)", null, "long"));
        verifyDataRows(result, rows(3000000000L * 2));

        result = executeQuery("select CAST(1.6 AS FLOAT) * 2");
        verifySchema(result, schema("CAST(1.6 AS FLOAT) * 2", null, "float"));
        verifyDataRows(result, rows(1.6 * 2));
    }

    @Test
    public void testMultiplyFunction() throws IOException {
        JSONObject result = executeQuery("select multiply(3, 2)");
        verifySchema(result, schema("multiply(3, 2)", null, "integer"));
        verifyDataRows(result, rows(3 * 2));

        result = executeQuery("select multiply(2.5, 2)");
        verifySchema(result, schema("multiply(2.5, 2)", null, "double"));
        verifyDataRows(result, rows(2.5D * 2));

        result = executeQuery("select multiply(3000000000, 2)");
        verifySchema(result, schema("multiply(3000000000, 2)", null, "long"));
        verifyDataRows(result, rows(3000000000L * 2));

        result = executeQuery("select multiply(CAST(1.6 AS FLOAT), 2)");
        verifySchema(result, schema("multiply(CAST(1.6 AS FLOAT), 2)", null, "float"));
        verifyDataRows(result, rows(1.6 * 2));
    }

    public void testSubtract() throws IOException {
        JSONObject result = executeQuery("select 3 - 2");
        verifySchema(result, schema("3 - 2", null, "integer"));
        verifyDataRows(result, rows(3 - 2));

        result = executeQuery("select 2.5 - 2");
        verifySchema(result, schema("2.5 - 2", null, "double"));
        verifyDataRows(result, rows(2.5D - 2));

        result = executeQuery("select 3000000000 - 2");
        verifySchema(result, schema("3000000000 - 2)", null, "long"));
        verifyDataRows(result, rows(3000000000L - 2));

        result = executeQuery("select CAST(1.6 AS FLOAT) - 2");
        verifySchema(result, schema("CAST(1.6 AS FLOAT) - 2", null, "float"));
        verifyDataRows(result, rows(1.6 - 2));
    }

    @Test
    public void testSubtractFunction() throws IOException {
        JSONObject result = executeQuery("select subtract(3, 2)");
        verifySchema(result, schema("subtract(3, 2)", null, "integer"));
        verifyDataRows(result, rows(3 - 2));

        result = executeQuery("select subtract(2.5, 2)");
        verifySchema(result, schema("subtract(2.5, 2)", null, "double"));
        verifyDataRows(result, rows(2.5D - 2));

        result = executeQuery("select subtract(3000000000, 2)");
        verifySchema(result, schema("subtract(3000000000, 2)", null, "long"));
        verifyDataRows(result, rows(3000000000L - 2));

        result = executeQuery("select subtract(CAST(1.6 AS FLOAT), 2)");
        verifySchema(result, schema("subtract(CAST(1.6 AS FLOAT), 2)", null, "float"));
        verifyDataRows(result, rows(1.6 - 2));
    }
}
