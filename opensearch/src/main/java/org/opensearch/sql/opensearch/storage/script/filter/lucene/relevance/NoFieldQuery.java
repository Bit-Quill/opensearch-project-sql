/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;

/**
 *  Base class to represent relevance queries that search multiple fields.
 * @param <T> The builder class for the OpenSearch query.
 */
abstract class NoFieldQuery<T extends QueryBuilder> extends RelevanceQuery<T> {

    public NoFieldQuery(Map<String, QueryBuilderStep<T>> queryBuildActions) {
        super(queryBuildActions);
    }

    /**
     * Override build function because RelevanceQuery requires 2 fields,
     * but NoFieldQuery must have no fields.
     * @param func : Contains function name and passed in arguments.
     * @return : QueryBuilder object
     */
    @Override
    public QueryBuilder build(FunctionExpression func) {
        var arguments = func.getArguments().stream()
                .map(a -> (NamedArgumentExpression)a).collect(Collectors.toList());
        if (arguments.size() < 1) {
            throw new SyntaxCheckException(
                    String.format("%s requires at least two parameters", getQueryName()));
        }

        // Aggregate parameters by name, so getting a Map<Name:String, List>
        arguments.stream().collect(Collectors.groupingBy(a -> a.getArgName().toLowerCase()))
                .forEach((k, v) -> {
                    if (v.size() > 1) {
                        throw new SemanticCheckException(
                                String.format("Parameter '%s' can only be specified once.", k));
                    }
                });

        T queryBuilder = createQueryBuilder(arguments);

        arguments.removeIf(a -> a.getArgName().equalsIgnoreCase("query"));

        var iterator = arguments.listIterator();
        while (iterator.hasNext()) {
            NamedArgumentExpression arg = iterator.next();
            String argNormalized = arg.getArgName().toLowerCase();
            String exceptionMessage;

            // This is required for query function as there is a mismatch in SQL function name
            // and query function name to OpenSearch
            if (queryBuilder.getWriteableName().equals("query_string")) {
                exceptionMessage = String.format("Parameter %s is invalid for query function.",
                        argNormalized, queryBuilder.getWriteableName());
            } else {
                exceptionMessage = String.format("Parameter %s is invalid for %s function.",
                        argNormalized, queryBuilder.getWriteableName());
            }

            if (!getQueryBuildActions().containsKey(argNormalized)) {
                throw new SemanticCheckException(exceptionMessage);
            }
            (Objects.requireNonNull(
                    getQueryBuildActions()
                            .get(argNormalized)))
                    .apply(queryBuilder, arg.getValue().valueOf(null));
        }
        return queryBuilder;
    }


    @Override
    public T createQueryBuilder(List<NamedArgumentExpression> arguments) {
        // Extract 'query'
        var query = arguments.stream()
                .filter(a -> a.getArgName().equalsIgnoreCase("query"))
                .findFirst()
                .orElseThrow(() -> new SemanticCheckException("'query' parameter is missing"));

        return createBuilder(query.getValue().valueOf(null).stringValue());
    }

    protected abstract  T createBuilder(String query);
}
