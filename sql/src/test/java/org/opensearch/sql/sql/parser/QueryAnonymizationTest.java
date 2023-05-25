/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.parser;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.sql.antlr.AnonymizerListener;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryAnonymizationTest extends SQLSyntaxParser {
    public AnonymizerListener getAnonymizer() {
        return anonymizer;
    }

    @Test
    public void queriesShouldHaveAnonymousFieldAndIndex() {
        String query = "SELECT ABS(balance) FROM accounts WHERE age > 30 GROUP BY ABS(balance)";
        String expectedQuery = "( SELECT ABS ( identifier ) FROM table WHERE identifier > number GROUP BY ABS ( identifier ) )";
        parse(query);
        assertEquals(expectedQuery, getAnonymizer().getAnonymizedQueryString());
    }

    @Test
    public void queriesShouldAnonymousNumbers() {
        String query = "SELECT ABS(20), LOG(20.20) FROM accounts";
        String expectedQuery = "( SELECT ABS ( number ) , LOG ( number ) FROM table )";
        parse(query);
        assertEquals(expectedQuery, getAnonymizer().getAnonymizedQueryString());
    }

    @Test
    public void queriesShouldHaveAnonymousBooleanLiterals() {
        String query = "SELECT TRUE FROM accounts";
        String expectedQuery = "( SELECT boolean_literal FROM table )";
        parse(query);
        assertEquals(expectedQuery, getAnonymizer().getAnonymizedQueryString());
    }

    @Test
    public void queriesShouldHaveAnonymousInputStrings() {
        String query = "SELECT * FROM accounts WHERE name = 'Oliver'";
        String expectedQuery = "( SELECT * FROM table WHERE identifier = 'string_literal' )";
        parse(query);
        assertEquals(expectedQuery, getAnonymizer().getAnonymizedQueryString());
    }

    @Test
    public void queriesWithAliasesShouldAnonymizeSensitiveData() {
        String query = "SELECT balance AS b FROM accounts AS a";
        String expectedQuery = "( SELECT identifier AS identifier FROM table AS identifier )";
        parse(query);
        assertEquals(expectedQuery, getAnonymizer().getAnonymizedQueryString());
    }

    @Test
    public void queriesWithFunctionsShouldAnonymizeSensitiveData() {
        String query = "SELECT LTRIM(firstname) FROM accounts";
        String expectedQuery = "( SELECT LTRIM ( identifier ) FROM table )";
        parse(query);
        assertEquals(expectedQuery, getAnonymizer().getAnonymizedQueryString());
    }

    @Test
    public void queriesWithAggregatesShouldAnonymizeSensitiveData() {
        String query = "SELECT MAX(price) - MIN(price) from tickets";
        String expectedQuery = "( SELECT MAX ( identifier ) - MIN ( identifier ) FROM table )";
        parse(query);
        assertEquals(expectedQuery, getAnonymizer().getAnonymizedQueryString());
    }

    @Test
    public void queriesWithSubqueriesShouldAnonymizeSensitiveData() {
        String query = "SELECT a.f, a.l, a.a FROM " +
                "(SELECT firstname AS f, lastname AS l, age AS a FROM accounts WHERE age > 30) a";
        String expectedQuery = "( SELECT identifier . identifier , identifier . identifier , identifier . identifier FROM " +
                "( SELECT identifier AS identifier , identifier AS identifier , identifier AS identifier " +
                "FROM table WHERE identifier > number ) identifier )";
        parse(query);
        assertEquals(expectedQuery, getAnonymizer().getAnonymizedQueryString());
    }
}
