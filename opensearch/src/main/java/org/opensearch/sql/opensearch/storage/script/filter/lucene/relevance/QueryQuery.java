/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.NamedArgumentExpression;

import java.util.List;


/**
 * Class for Lucene query that builds the query_string query.
 */
public class QueryQuery extends QueryStringQuery {
    /**
     *  Default constructor for QueryQuery configures how RelevanceQuery.build() handles
     * named arguments by calling the constructor of QueryStringQuery.
     */
    public QueryQuery(){
        super();
    }

    /**
     * Builds QueryBuilder with query value and other default parameter values set.
     *
     * @param query : Query value for query_string query
     * @return : Builder for query query
     */
    protected QueryStringQueryBuilder createBuilder(String query) {
        return QueryBuilders.queryStringQuery(query);
    }

    @Override
    protected String getQueryName() {
        return QueryStringQueryBuilder.NAME;
    }

    /**
     * Overrides the parent inherited function as it should not have a field parameter.
     *
     * @param arguments : Query value
     * @return : Builder for query_string query
     */
    @Override
    public QueryStringQueryBuilder createQueryBuilder(List<NamedArgumentExpression> arguments) {
        // Extract 'query'
        var query = arguments.stream()
                .filter(a -> a.getArgName().equalsIgnoreCase("query"))
                .findFirst()
                .orElseThrow(() -> new SemanticCheckException("'query' parameter is missing"));

        return createBuilder(query.getValue().valueOf(null).stringValue());
    }
}