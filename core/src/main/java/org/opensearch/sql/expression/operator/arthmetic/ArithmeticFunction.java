/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.operator.arthmetic;

import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;

/**
 * The definition of arithmetic function
 * add, Accepts two numbers and produces a number.
 * subtract, Accepts two numbers and produces a number.
 * multiply, Accepts two numbers and produces a number.
 * divide, Accepts two numbers and produces a number.
 * module, Accepts two numbers and produces a number.
 */
@UtilityClass
public class ArithmeticFunction {
  /**
   * Register Arithmetic Function.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(add());
    repository.register(subtract());
    repository.register(multiply());
    repository.register(divide());
    repository.register(modules());
  }

  private static DefaultFunctionResolver add() {
    return define(BuiltinFunctionName.ADD.getName(),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprByteValue(v1.byteValue() + v2.byteValue())),
            BYTE, BYTE, BYTE),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprShortValue(v1.shortValue() + v2.shortValue())),
            SHORT, SHORT, SHORT),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprIntegerValue(Math.addExact(v1.integerValue(), v2.integerValue()))),
            INTEGER, INTEGER, INTEGER),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprLongValue(Math.addExact(v1.longValue(), v2.longValue()))),
            LONG, LONG, LONG),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprFloatValue(v1.floatValue() + v2.floatValue())),
            FLOAT, FLOAT, FLOAT),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprDoubleValue(v1.doubleValue() + v2.doubleValue())),
            DOUBLE, DOUBLE, DOUBLE)
    );
  }

  private static DefaultFunctionResolver subtract() {
    return define(BuiltinFunctionName.SUBTRACT.getName(),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprByteValue(v1.byteValue() - v2.byteValue())),
            BYTE, BYTE, BYTE),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprShortValue(v1.shortValue() - v2.shortValue())),
            SHORT, SHORT, SHORT),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprIntegerValue(Math.subtractExact(v1.integerValue(),
                v2.integerValue()))),
            INTEGER, INTEGER, INTEGER),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprLongValue(Math.subtractExact(v1.longValue(), v2.longValue()))),
            LONG, LONG, LONG),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprFloatValue(v1.floatValue() - v2.floatValue())),
            FLOAT, FLOAT, FLOAT),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprDoubleValue(v1.doubleValue() - v2.doubleValue())),
            DOUBLE, DOUBLE, DOUBLE)
    );
  }

  private static DefaultFunctionResolver multiply() {
    return define(BuiltinFunctionName.MULTIPLY.getName(),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprByteValue(v1.byteValue() * v2.byteValue())),
            BYTE, BYTE, BYTE),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprShortValue(v1.shortValue() * v2.shortValue())),
            SHORT, SHORT, SHORT),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprIntegerValue(Math.multiplyExact(v1.integerValue(),
                v2.integerValue()))),
            INTEGER, INTEGER, INTEGER),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprLongValue(Math.multiplyExact(v1.longValue(), v2.longValue()))),
            LONG, LONG, LONG),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprFloatValue(v1.floatValue() * v2.floatValue())),
            FLOAT, FLOAT, FLOAT),
        impl(nullMissingHandling(
            (v1, v2) -> new ExprDoubleValue(v1.doubleValue() * v2.doubleValue())),
            DOUBLE, DOUBLE, DOUBLE)
    );
  }

  private static DefaultFunctionResolver divide() {
    return define(BuiltinFunctionName.DIVIDE.getName(),
        impl(nullMissingHandling(
            (v1, v2) -> v2.byteValue() == 0 ? ExprNullValue.of() :
                new ExprByteValue(v1.byteValue() / v2.byteValue())),
            BYTE, BYTE, BYTE),
        impl(nullMissingHandling(
            (v1, v2) -> v2.shortValue() == 0 ? ExprNullValue.of() :
                new ExprShortValue(v1.shortValue() / v2.shortValue())),
            SHORT, SHORT, SHORT),
        impl(nullMissingHandling(
            (v1, v2) -> v2.integerValue() == 0 ? ExprNullValue.of() :
                new ExprIntegerValue(v1.integerValue() / v2.integerValue())),
            INTEGER, INTEGER, INTEGER),
        impl(nullMissingHandling(
            (v1, v2) -> v2.longValue() == 0 ? ExprNullValue.of() :
                new ExprLongValue(v1.longValue() / v2.longValue())),
            LONG, LONG, LONG),
        impl(nullMissingHandling(
            (v1, v2) -> v2.floatValue() == 0 ? ExprNullValue.of() :
                new ExprFloatValue(v1.floatValue() / v2.floatValue())),
            FLOAT, FLOAT, FLOAT),
        impl(nullMissingHandling(
            (v1, v2) -> v2.doubleValue() == 0 ? ExprNullValue.of() :
                new ExprDoubleValue(v1.doubleValue() / v2.doubleValue())),
            DOUBLE, DOUBLE, DOUBLE)
    );
  }

  private static DefaultFunctionResolver modules() {
    return define(BuiltinFunctionName.MODULES.getName(),
        impl(nullMissingHandling(
            (v1, v2) -> v2.byteValue() == 0 ? ExprNullValue.of() :
                new ExprByteValue(v1.byteValue() % v2.byteValue())),
            BYTE, BYTE, BYTE),
        impl(nullMissingHandling(
            (v1, v2) -> v2.shortValue() == 0 ? ExprNullValue.of() :
                new ExprShortValue(v1.shortValue() % v2.shortValue())),
            SHORT, SHORT, SHORT),
        impl(nullMissingHandling(
            (v1, v2) -> v2.integerValue() == 0 ? ExprNullValue.of() :
                new ExprIntegerValue(v1.integerValue() % v2.integerValue())),
            INTEGER, INTEGER, INTEGER),
        impl(nullMissingHandling(
            (v1, v2) -> v2.longValue() == 0 ? ExprNullValue.of() :
                new ExprLongValue(v1.longValue() % v2.longValue())),
            LONG, LONG, LONG),
        impl(nullMissingHandling(
            (v1, v2) -> v2.floatValue() == 0 ? ExprNullValue.of() :
                new ExprFloatValue(v1.floatValue() % v2.floatValue())),
            FLOAT, FLOAT, FLOAT),
        impl(nullMissingHandling(
            (v1, v2) -> v2.doubleValue() == 0 ? ExprNullValue.of() :
                new ExprDoubleValue(v1.doubleValue() % v2.doubleValue())),
            DOUBLE, DOUBLE, DOUBLE)
    );
  }
}
