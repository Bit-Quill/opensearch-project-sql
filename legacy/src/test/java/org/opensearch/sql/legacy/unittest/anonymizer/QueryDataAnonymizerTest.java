/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.unittest.anonymizer;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.legacy.rewriter.identifier.AnonymizeSensitiveDataRule;
import org.opensearch.sql.legacy.utils.Util;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.parser.AstBuilder;
import org.opensearch.sql.sql.parser.AstStatementBuilder;
import org.opensearch.sql.sql.parser.SQLQueryDataAnonymizer;
import org.opensearch.sql.sql.parser.SQLQueryDataAnonymizer2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class QueryDataAnonymizerTest {
    static File currentDirectoryFile = new File(".");
    static String absolutePath = currentDirectoryFile.getAbsolutePath();
    static final int RELATIVE_PATH_LENGTH = 9;
    static String projectDirectory = absolutePath.substring(0, absolutePath.length() - RELATIVE_PATH_LENGTH);
    static String TEST_DATA_LOCATION = "integ-test/src/test/resources/correctness/queries/";
    static List<String> testDataFiles = Arrays.asList(
            //"aggregation",
            //"filter",
            //"groupby",
            //"joins",
            "orderby"
            //"select",
            //"subqueries",
            //"window"
    );

    private final String queryData;

    public QueryDataAnonymizerTest(String queryData) {
        this.queryData = queryData;
    }

    public String anonymizeData(String query) {
        SQLSyntaxParser parser = new SQLSyntaxParser();
        ParseTree cst = parser.parse(query);
        Statement statement =
                cst.accept(
                        new AstStatementBuilder(
                                new AstBuilder(query),
                                AstStatementBuilder.StatementBuilderContext.builder().build()));
        SQLQueryDataAnonymizer anonymizer = new SQLQueryDataAnonymizer();
        return anonymizer.anonymizeStatement(statement);
    }

    public String legacyAnonymizeData(String query) {
        AnonymizeSensitiveDataRule rule = new AnonymizeSensitiveDataRule();
        SQLQueryExpr sqlExpr = (SQLQueryExpr) Util.toSqlExpr(query);
        rule.rewrite(sqlExpr);
        return SQLUtils.toMySqlString(sqlExpr).replaceAll("0", "number")
                .replaceAll("false", "boolean_literal")
                .replaceAll("[\\n][\\t]+", " ");
    }

    public static List<String> loadData(String dataLocation) throws IOException {
        return Files.readAllLines(
                Paths.get(projectDirectory,
                        TEST_DATA_LOCATION, dataLocation).normalize());
    }

    @Parameterized.Parameters
    public static Collection<String> data() throws IOException {
        List<String> queryList = new ArrayList<>();

        for (String testDataFile : testDataFiles) {
            queryList.addAll(loadData(testDataFile + ".txt"));
        }

        return queryList;
    }

    @Test
    public void shouldReturnCorrectSum() {
        assertEquals(legacyAnonymizeData(queryData), anonymizeData(queryData));
    }
}
