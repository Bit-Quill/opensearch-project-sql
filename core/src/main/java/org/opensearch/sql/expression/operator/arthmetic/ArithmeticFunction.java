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

import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionDSL;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.SerializableBiFunction;
import org.opensearch.sql.expression.function.SerializableFunction;

import java.math.BigDecimal;

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
    repository.register(addFunction());
    repository.register(divide());
    repository.register(divideFunction());
    repository.register(mod());
    repository.register(modulus());
    repository.register(modulusFunction());
    repository.register(multiply());
    repository.register(multiplyFunction());
    repository.register(subtract());
    repository.register(subtractFunction());
  }

  private static SerializableFunction baseArithmeticFunction(SerializableBiFunction<BigDecimal, BigDecimal, BigDecimal> formula,
                                                             Boolean secondArgumentZeroCheck,
                                                             SerializableFunction<ExprValue, BigDecimal> argValue,
                                                             SerializableFunction<BigDecimal, Number> convertValue,
                                                             ExprCoreType dataType) {
    return FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(
                    (ExprValue v1, ExprValue v2) -> (secondArgumentZeroCheck && v2.byteValue() == 0) ? ExprNullValue.of() :
                            ExprValueUtils.fromObjectValue(convertValue.apply(formula.apply(argValue.apply(v1), argValue.apply(v2))))),
            dataType, dataType, dataType);
  }

  private static DefaultFunctionResolver baseArithmeticParser(SerializableBiFunction<BigDecimal, BigDecimal, BigDecimal> formula,
                                                      boolean secondArgumentZeroCheck, FunctionName functionName) {
    return FunctionDSL.define(functionName,
            baseArithmeticFunction(formula, secondArgumentZeroCheck,
                    v -> BigDecimal.valueOf(v.byteValue()),
                    v -> new ExprByteValue(v).byteValue(), BYTE),
            baseArithmeticFunction(formula, secondArgumentZeroCheck,
                    v -> BigDecimal.valueOf(v.shortValue()),
                    v -> new ExprShortValue(v).shortValue(), SHORT),
            baseArithmeticFunction(formula, secondArgumentZeroCheck,
                    v -> BigDecimal.valueOf(v.integerValue()),
                    v -> new ExprIntegerValue(v).integerValue(), INTEGER),
            baseArithmeticFunction(formula, secondArgumentZeroCheck,
                    v -> BigDecimal.valueOf(v.longValue()),
                    v -> new ExprLongValue(v).longValue(), LONG),
            baseArithmeticFunction(formula, secondArgumentZeroCheck,
                    v -> BigDecimal.valueOf(v.floatValue()),
                    v -> new ExprFloatValue(v).floatValue(), FLOAT),
            baseArithmeticFunction(formula, secondArgumentZeroCheck,
                    v -> BigDecimal.valueOf(v.doubleValue()),
                    v -> new ExprDoubleValue(v).doubleValue(), DOUBLE)
    );
  }

  /**
   * Definition of add(x, y) function.
   * Returns the number x plus number y
   * The supported signature of add function is
   * (x: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE, y: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE)
   * -> wider type between types of x and y
   */
  private static DefaultFunctionResolver add() {
    return baseArithmeticParser(BigDecimal::add, false, BuiltinFunctionName.ADD.getName());
  }

  private static DefaultFunctionResolver addFunction() {
    return baseArithmeticParser(BigDecimal::add, false, BuiltinFunctionName.ADDFUNCTION.getName());
  }

  /**
   * Definition of divide(x, y) function.
   * Returns the number x divided by number y
   * The supported signature of divide function is
   * (x: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE, y: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE)
   * -> wider type between types of x and y
   */
  private static DefaultFunctionResolver divide() {
    return baseArithmeticParser(BigDecimal::divide, true, BuiltinFunctionName.DIVIDE.getName());
  }

  private static DefaultFunctionResolver divideFunction() {
    return baseArithmeticParser(BigDecimal::divide, true, BuiltinFunctionName.DIVIDEFUNCTION.getName());
  }

  /**
   * Definition of modulo(x, y) function.
   * Returns the number x modulo by number y
   * The supported signature of modulo function is
   * (x: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE, y: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE)
   * -> wider type between types of x and y
   */
  private static DefaultFunctionResolver mod() {
    return baseArithmeticParser(BigDecimal::remainder, true, BuiltinFunctionName.MOD.getName());
  }

  private static DefaultFunctionResolver modulus() {
    return baseArithmeticParser(BigDecimal::remainder, true, BuiltinFunctionName.MODULUS.getName());
  }

  private static DefaultFunctionResolver modulusFunction() {
    return baseArithmeticParser(BigDecimal::remainder, true, BuiltinFunctionName.MODULUSFUNCTION.getName());
  }

  /**
   * Definition of multiply(x, y) function.
   * Returns the number x multiplied by number y
   * The supported signature of multiply function is
   * (x: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE, y: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE)
   * -> wider type between types of x and y
   */
  private static DefaultFunctionResolver multiply() {
    return baseArithmeticParser(BigDecimal::multiply, false, BuiltinFunctionName.MULTIPLY.getName());
  }

  private static DefaultFunctionResolver multiplyFunction() {
    return baseArithmeticParser(BigDecimal::multiply, false, BuiltinFunctionName.MULTIPLYFUNCTION.getName());
  }

  /**
   * Definition of subtract(x, y) function.
   * Returns the number x minus number y
   * The supported signature of subtract function is
   * (x: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE, y: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE)
   * -> wider type between types of x and y
   */
  private static DefaultFunctionResolver subtract() {
    return baseArithmeticParser(BigDecimal::subtract, false, BuiltinFunctionName.SUBTRACT.getName());
  }

  private static DefaultFunctionResolver subtractFunction() {
    return baseArithmeticParser(BigDecimal::subtract, false, BuiltinFunctionName.SUBTRACTFUNCTION.getName());
  }
}
