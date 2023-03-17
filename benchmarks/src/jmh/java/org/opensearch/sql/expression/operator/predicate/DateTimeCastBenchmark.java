/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.predicate;

import com.google.common.collect.ImmutableMap;
import org.openjdk.jmh.annotations.*;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.opensearch.sql.data.model.ExprValueUtils.*;
import static org.opensearch.sql.data.type.ExprCoreType.*;
import static org.opensearch.sql.expression.DSL.literal;

@Warmup(iterations = 1)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class DateTimeCastBenchmark {

    @Param(value = { "string", "timestamp" })
    private String testDataType;

    private final Map<String, ExprValue> params =
            ImmutableMap.<String, ExprValue>builder()
                    .put("string", fromObjectValue("2022-01-12 14:59:26", STRING))
                    .put("timestamp", fromObjectValue("2022-01-12 14:59:26", TIMESTAMP))
                    .build();

    @Benchmark
    public void testCast() {
        run(DSL::castTimestamp);
    }

    @Benchmark
    public void testHardCast() {
        run(DSL::timestamp);
    }

    private void run(Function<Expression, FunctionExpression> dsl) {
        ExprValue param = params.get(testDataType);
        FunctionExpression func = dsl.apply(literal(param));
        func.valueOf();
    }
}
