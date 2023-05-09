/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.utils;

import static org.opensearch.sql.legacy.utils.Util.toSqlExpr;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.legacy.rewriter.identifier.AnonymizeSensitiveDataRule;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.parser.AstBuilder;
import org.opensearch.sql.sql.parser.AstStatementBuilder;
import org.opensearch.sql.sql.parser.SQLQueryDataAnonymizer;
import org.opensearch.sql.sql.parser.SQLQueryDataAnonymizer2;

/**
 * Utility class to mask sensitive information in incoming SQL queries
 */
public class QueryDataAnonymizer {

    /**
     * This method is used to anonymize sensitive data in SQL query.
     * Sensitive data includes index names, column names etc.,
     * which in druid parser are parsed to SQLIdentifierExpr instances
     * @param query entire sql query string
     * @return sql query string with all identifiers replaced with "***" on success
     * and failure string otherwise to ensure no non-anonymized data is logged in production.
     */
    public static String anonymizeData(String query) {
        String resultQuery;
        try {
            throw new Exception();
            /*
            AnonymizeSensitiveDataRule rule = new AnonymizeSensitiveDataRule();
            SQLQueryExpr sqlExpr = (SQLQueryExpr) toSqlExpr(query);
            rule.rewrite(sqlExpr);
            resultQuery = SQLUtils.toMySqlString(sqlExpr).replaceAll("0", "number")
                    .replaceAll("false", "boolean_literal")
                    .replaceAll("[\\n][\\t]+", " ");
             */
        } catch (Exception e) {
            SQLSyntaxParser parser = new SQLSyntaxParser();
            ParseTree cst = parser.parse(query);
            Statement statement =
                    cst.accept(
                            new AstStatementBuilder(
                                    new AstBuilder(query),
                                    AstStatementBuilder.StatementBuilderContext.builder().build()));
            SQLQueryDataAnonymizer2 anonymizer = new SQLQueryDataAnonymizer2();
            resultQuery = anonymizer.anonymizeStatement(statement);
        }

        return resultQuery;
    }
}
