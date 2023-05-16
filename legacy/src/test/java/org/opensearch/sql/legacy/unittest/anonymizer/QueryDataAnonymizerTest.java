/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.unittest.anonymizer;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.utils.QueryDataAnonymizer.anonymizeData;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.*;

@RunWith(Parameterized.class)
public class QueryDataAnonymizerTest {
    static File currentDirectoryFile = new File(".");
    static String absolutePath = currentDirectoryFile.getAbsolutePath();
    static final int RELATIVE_PATH_LENGTH = 9;
    static String projectDirectory = absolutePath.substring(0, absolutePath.length() - RELATIVE_PATH_LENGTH);
    static String TEST_DATA_LOCATION = "integ-test/src/test/resources/correctness/queries/";
    static List<String> testDataFiles = Arrays.asList(
            "aggregation",
            "filter",
            "groupby",
            "joins",
            "orderby",
            "select",
            "subqueries",
            "window"
    );

    private final String queryData;

    public QueryDataAnonymizerTest(String queryData) {
        this.queryData = queryData;
    }

    public String anonymizeNewData(String query) {
        SQLSyntaxParser parser = new SQLSyntaxParser();

        OpenSearchSQLParser cstParser = parser.createParser(query);
        AnonymizerListener anonymizer = new AnonymizerListener();
        cstParser.addParseListener(anonymizer);
        cstParser.root();
        return anonymizer.getAnonymizedQueryString();
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
        assertEquals(anonymizeData(queryData), anonymizeNewData(queryData));
    }

    public class AnonymizerListener implements ParseTreeListener {
        private String anonymizedQueryString = "";
        private int previousType = -1;

        @Override
        public void enterEveryRule(ParserRuleContext ctx) {
        }

        @Override
        public void visitTerminal(TerminalNode node) {
            System.out.println(node.getText());

            if (node.getSymbol().getType() != RR_BRACKET &&
                    (node.getSymbol().getType() != LR_BRACKET || previousType == FROM) &&
                    node.getSymbol().getType() != COMMA &&
                    node.getSymbol().getType() != DOT &&
                    previousType != LR_BRACKET &&
                    previousType != DOT &&
                    previousType != -1) {
                anonymizedQueryString += " ";
            }

            switch(node.getSymbol().getType()) {
                case ID:
                case TIMESTAMP:
                case BACKTICK_QUOTE_ID:
                    anonymizedQueryString += "identifier";
                    break;
                case ZERO_DECIMAL:
                case ONE_DECIMAL:
                case TWO_DECIMAL:
                case DECIMAL_LITERAL:
                    anonymizedQueryString += "number";
                    break;
                case STRING_LITERAL:
                    anonymizedQueryString += "'string_literal'";
                    break;
                case -1:
                    // end of file
                    break;
                default:
                    anonymizedQueryString += node.getText().toUpperCase();
            }
            previousType = node.getSymbol().getType();
        }

        @Override
        public void visitErrorNode(ErrorNode node) {
        }

        @Override
        public void exitEveryRule(ParserRuleContext ctx) {
        }

        public String getAnonymizedQueryString() {
            return "( " + anonymizedQueryString + ")";
        }
    }
}
