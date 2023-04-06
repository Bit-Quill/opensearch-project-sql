/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * Lucene query abstraction that builds Lucene query from function expression.
 */
public abstract class LuceneQuery {

  /**
   * Check if function expression supported by current Lucene query.
   * Default behavior is that report supported if:
   *  1. Left is a reference
   *  2. Right side is a literal
   *
   * @param func    function
   * @return        return true if supported, otherwise false.
   */
  public boolean canSupport(FunctionExpression func) {
    return (func.getArguments().size() == 2)
        && (func.getArguments().get(0) instanceof ReferenceExpression)
        && (func.getArguments().get(1) instanceof LiteralExpression
        || literalExpressionWrappedByCast(func))
        || isMultiParameterQuery(func)
        || isNestedFunction(func);
  }

  private boolean isNestedFunction(FunctionExpression func) {
    return (func.getFunctionName().getFunctionName().equalsIgnoreCase("nested")
        || ((FunctionExpression)func.getArguments().get(0)).getFunctionName().getFunctionName().equalsIgnoreCase("nested"));
  }

  /**
   * Check if the function expression has multiple named argument expressions as the parameters.
   *
   * @param func      function
   * @return          return true if the expression is a multi-parameter function.
   */
  private boolean isMultiParameterQuery(FunctionExpression func) {
    for (Expression expr : func.getArguments()) {
      if (!(expr instanceof NamedArgumentExpression)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if the second argument of the function is a literal expression wrapped by cast function.
   */
  private boolean literalExpressionWrappedByCast(FunctionExpression func) {
    if (func.getArguments().get(1) instanceof FunctionExpression) {
      FunctionExpression expr = (FunctionExpression) func.getArguments().get(1);
      return castMap.containsKey(expr.getFunctionName())
          && expr.getArguments().get(0) instanceof LiteralExpression;
    }
    return false;
  }

  /**
   * Build Lucene query from function expression.
   * The cast function is converted to literal expressions before generating DSL.
   *
   * @param func  function
   * @return      query
   */
  public QueryBuilder build(FunctionExpression func) {
    ReferenceExpression ref = (ReferenceExpression) func.getArguments().get(0);
    Expression expr = func.getArguments().get(1);
    ExprValue literalValue = expr instanceof LiteralExpression ? expr
        .valueOf() : cast((FunctionExpression) expr);
    return doBuild(ref.getAttr(), ref.type(), literalValue);
  }

  public QueryBuilder build(FunctionExpression func, BiFunction<BoolQueryBuilder, QueryBuilder,
        QueryBuilder> accumulator) {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    FunctionExpression funcOrMultipleFuncs = (FunctionExpression) func.getArguments().get(1);
    if (func.getFunctionName().getFunctionName().equalsIgnoreCase("nested")) {
      switch(funcOrMultipleFuncs.getFunctionName().getFunctionName()) {
        case "and":
        case "or":
        case "not":
          for (var f : func.getArguments()) {
            switch (funcOrMultipleFuncs.getFunctionName().getFunctionName()) {
              case "and":
                accumulator.apply(boolQuery, doBuild(func));
              case "or":
                accumulator.apply(boolQuery, doBuild(func));
              case "not":
                accumulator.apply(boolQuery, doBuild(func));
              default:
            }
          }
        break;

        default:
          accumulator.apply(boolQuery, doBuild(func));
      }

//      for (int i = 0; i < ((FunctionExpression)func.getArguments().get(1)).getArguments().size(); i++) {
////        FunctionExpression expr = (FunctionExpression) ((FunctionExpression)func.getArguments().get(1)).getArguments().get(i);
//        accumulator.apply(boolQuery, doBuild(func));
//      }
    } else if (((FunctionExpression)func.getArguments().get(0)).getFunctionName().getFunctionName().equalsIgnoreCase("nested")) { // Is predicate expression
      // TODO If function doesnt contains conditional we should throw exception.
      QueryBuilder ret = doBuild(func);
      accumulator.apply(boolQuery, ret);
    }
//      // Conditional with only one predicate.
//      if (((FunctionExpression)func.getArguments().get(1)).getArguments().get(0) instanceof ReferenceExpression
//          && ((FunctionExpression)func.getArguments().get(1)).getArguments().get(1) instanceof LiteralExpression) {
//        queries.add(doBuild((FunctionExpression)func.getArguments().get(1), path));
//      } else {
//        // Multiple predicates. Only enters if more than one predicate.
//        for (int i = 0; i < ((FunctionExpression)func.getArguments().get(1)).getArguments().size(); i++) {
//          FunctionExpression expr = (FunctionExpression) ((FunctionExpression)func.getArguments().get(1)).getArguments().get(i);
//          queries.add(doBuild(expr, path));
//        }
//      }
//
//      for (QueryBuilder query : queries) {
//        accumulator.apply(boolQuery, query);
//      }
//    }
    return boolQuery;
  }

  void blahInnerFuncs(FunctionExpression func, BiFunction<BoolQueryBuilder, QueryBuilder,
      QueryBuilder> accumulator) {

  }


  private ExprValue cast(FunctionExpression castFunction) {
    return castMap.get(castFunction.getFunctionName()).apply(
        (LiteralExpression) castFunction.getArguments().get(0));
  }

  /**
   * Type converting map.
   */
  private final Map<FunctionName, Function<LiteralExpression, ExprValue>> castMap = ImmutableMap
      .<FunctionName, Function<LiteralExpression, ExprValue>>builder()
      .put(BuiltinFunctionName.CAST_TO_STRING.getName(), expr -> {
        if (!expr.type().equals(ExprCoreType.STRING)) {
          return new ExprStringValue(String.valueOf(expr.valueOf().value()));
        } else {
          return expr.valueOf();
        }
      })
      .put(BuiltinFunctionName.CAST_TO_BYTE.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprByteValue(expr.valueOf().byteValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprByteValue(expr.valueOf().booleanValue() ? 1 : 0);
        } else {
          return new ExprByteValue(Byte.valueOf(expr.valueOf().stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_SHORT.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprShortValue(expr.valueOf().shortValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprShortValue(expr.valueOf().booleanValue() ? 1 : 0);
        } else {
          return new ExprShortValue(Short.valueOf(expr.valueOf().stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_INT.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprIntegerValue(expr.valueOf().integerValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprIntegerValue(expr.valueOf().booleanValue() ? 1 : 0);
        } else {
          return new ExprIntegerValue(Integer.valueOf(expr.valueOf().stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_LONG.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprLongValue(expr.valueOf().longValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprLongValue(expr.valueOf().booleanValue() ? 1 : 0);
        } else {
          return new ExprLongValue(Long.valueOf(expr.valueOf().stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_FLOAT.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprFloatValue(expr.valueOf().floatValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprFloatValue(expr.valueOf().booleanValue() ? 1 : 0);
        } else {
          return new ExprFloatValue(Float.valueOf(expr.valueOf().stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_DOUBLE.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprDoubleValue(expr.valueOf().doubleValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprDoubleValue(expr.valueOf().booleanValue() ? 1 : 0);
        } else {
          return new ExprDoubleValue(Double.valueOf(expr.valueOf().stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_BOOLEAN.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return expr.valueOf().doubleValue() != 0
              ? ExprBooleanValue.of(true) : ExprBooleanValue.of(false);
        } else if (expr.type().equals(ExprCoreType.STRING)) {
          return ExprBooleanValue.of(Boolean.valueOf(expr.valueOf().stringValue()));
        } else {
          return expr.valueOf();
        }
      })
      .put(BuiltinFunctionName.CAST_TO_DATE.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprDateValue(expr.valueOf().stringValue());
        } else {
          return new ExprDateValue(expr.valueOf().dateValue());
        }
      })
      .put(BuiltinFunctionName.CAST_TO_TIME.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprTimeValue(expr.valueOf().stringValue());
        } else {
          return new ExprTimeValue(expr.valueOf().timeValue());
        }
      })
      .put(BuiltinFunctionName.CAST_TO_DATETIME.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprDatetimeValue(expr.valueOf().stringValue());
        } else {
          return new ExprDatetimeValue(expr.valueOf().datetimeValue());
        }
      })
      .put(BuiltinFunctionName.CAST_TO_TIMESTAMP.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprTimestampValue(expr.valueOf().stringValue());
        } else {
          return new ExprTimestampValue(expr.valueOf().timestampValue());
        }
      })
      .build();

  /**
   * Build method that subclass implements by default which is to build query
   * from reference and literal in function arguments.
   *
   * @param fieldName   field name
   * @param fieldType   field type
   * @param literal     field value literal
   * @return            query
   */
  protected QueryBuilder doBuild(String fieldName, ExprType fieldType, ExprValue literal) {
    throw new UnsupportedOperationException(
        "Subclass doesn't implement this and build method either");
  }

  protected QueryBuilder doBuild(FunctionExpression func) {
    throw new UnsupportedOperationException(
        "Subclass doesn't implement this and build method either");
  }
}
