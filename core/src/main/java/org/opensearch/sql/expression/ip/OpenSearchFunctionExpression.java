/*
 *
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.expression.ip;

import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;

import java.util.List;

/**
 * Marker interface to identify functions compatible when OpenSearch is being used as storage engine.
 */
public class OpenSearchFunctionExpression extends FunctionExpression {

    private final ExprType returnType;

    public OpenSearchFunctionExpression(FunctionName functionName, List<Expression> arguments, ExprType returnType) {
        super(functionName, arguments);
        this.returnType = returnType;
    }

    @Override
    public ExprValue valueOf() {
        return null;
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
        throw new UnsupportedOperationException(
                "OpenSearch runtime specific function, no default implementation available");
    }

    @Override
    public ExprType type() {
        return returnType;
    }
}
