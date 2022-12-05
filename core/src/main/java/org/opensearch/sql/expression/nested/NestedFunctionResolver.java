package org.opensearch.sql.expression.nested;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionDSL;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;

import java.util.List;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

@RequiredArgsConstructor
public class NestedFunctionResolver {
//  @Getter
//  private final FunctionName functionName;
//
//  @Getter
//  private final List<ExprCoreType> paramTypeList;
//
//  private static DefaultFunctionResolver nested() {
////    return FunctionDSL.define(BuiltinFunctionName.NESTED.getName(),
////        FunctionDSL.impl(
////            FunctionDSL.nullMissingHandling(v -> v))),
////    STRING, REF);
//    return null;
//  }
}
