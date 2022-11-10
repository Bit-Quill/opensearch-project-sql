/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.TypeEnvironment;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;

@UtilityClass
public class OpenSearchFunctions {
  private final List<String> singleFieldFunctionNames = ImmutableList.of(
      BuiltinFunctionName.MATCH.name(),
      BuiltinFunctionName.MATCH_BOOL_PREFIX.name(),
      BuiltinFunctionName.MATCHPHRASE.name(),
      BuiltinFunctionName.MATCH_PHRASE_PREFIX.name()
  );

  private final List<String> multiFieldFunctionNames = ImmutableList.of(
      BuiltinFunctionName.MULTI_MATCH.name(),
      BuiltinFunctionName.SIMPLE_QUERY_STRING.name(),
      BuiltinFunctionName.QUERY_STRING.name()
  );

  /**
   * Check if supplied function name is valid SingleFieldRelevanceFunction.
   * @param funcName : Name of function
   * @return : True if function is single-field function
   */
  public static boolean isSingleFieldFunction(String funcName) {
    return singleFieldFunctionNames.contains(funcName.toUpperCase());
  }

  /**
   * Check if supplied function name is valid MultiFieldRelevanceFunction.
   * @param funcName : Name of function
   * @return : True if function is multi-field function
   */
  public static boolean isMultiFieldFunction(String funcName) {
    return multiFieldFunctionNames.contains(funcName.toUpperCase());
  }

  /**
   * Verify if function queries fields available in type environment.
   * @param node : Function used in query.
   * @param context : Context of fields querying.
   */
  public static void validateFieldList(FunctionExpression node, AnalysisContext context) {
    String funcName = node.getFunctionName().toString();

    TypeEnvironment typeEnv = context.peek();
    if (isSingleFieldFunction(funcName)) {
      node.getArguments().stream().map(NamedArgumentExpression.class::cast).filter(arg ->
          ((arg.getArgName().equals("field")
              && !arg.getValue().toString().contains("*"))
          )).findFirst().ifPresent(arg ->
          typeEnv.resolve(new Symbol(Namespace.FIELD_NAME,
              StringUtils.unquoteText(arg.getValue().toString()))
          )
      );
    } else if (isMultiFieldFunction(funcName)) {
      node.getArguments().stream().map(NamedArgumentExpression.class::cast).filter(arg ->
          arg.getArgName().equals("fields")
      ).findFirst().ifPresent(fields ->
          fields.getValue().valueOf(null).tupleValue()
              .entrySet().stream().filter(k -> !(k.getKey().contains("*"))
              ).forEach(key -> typeEnv.resolve(new Symbol(Namespace.FIELD_NAME, key.getKey())))
      );
    }
  }

  /**
   * Add functions specific to OpenSearch to repository.
   */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(match_bool_prefix());
    repository.register(multi_match(BuiltinFunctionName.MULTI_MATCH));
    repository.register(multi_match(BuiltinFunctionName.MULTIMATCH));
    repository.register(multi_match(BuiltinFunctionName.MULTIMATCHQUERY));
    repository.register(match(BuiltinFunctionName.MATCH));
    repository.register(match(BuiltinFunctionName.MATCHQUERY));
    repository.register(match(BuiltinFunctionName.MATCH_QUERY));
    repository.register(simple_query_string());
    repository.register(query());
    repository.register(query_string());
    // Register MATCHPHRASE as MATCH_PHRASE as well for backwards
    // compatibility.
    repository.register(match_phrase(BuiltinFunctionName.MATCH_PHRASE));
    repository.register(match_phrase(BuiltinFunctionName.MATCHPHRASE));
    repository.register(match_phrase(BuiltinFunctionName.MATCHPHRASEQUERY));
    repository.register(match_phrase_prefix());
    repository.register(wildcard_query(BuiltinFunctionName.WILDCARD_QUERY));
    repository.register(wildcard_query(BuiltinFunctionName.WILDCARDQUERY));
  }

  private static FunctionResolver match_bool_prefix() {
    FunctionName name = BuiltinFunctionName.MATCH_BOOL_PREFIX.getName();
    return new RelevanceFunctionResolver(name, STRING);
  }

  private static FunctionResolver match(BuiltinFunctionName match) {
    FunctionName funcName = match.getName();
    return new RelevanceFunctionResolver(funcName, STRING);
  }

  private static FunctionResolver match_phrase_prefix() {
    FunctionName funcName = BuiltinFunctionName.MATCH_PHRASE_PREFIX.getName();
    return new RelevanceFunctionResolver(funcName, STRING);
  }

  private static FunctionResolver match_phrase(BuiltinFunctionName matchPhrase) {
    FunctionName funcName = matchPhrase.getName();
    return new RelevanceFunctionResolver(funcName, STRING);
  }

  private static FunctionResolver multi_match(BuiltinFunctionName multiMatchName) {
    return new RelevanceFunctionResolver(multiMatchName.getName(), STRUCT);
  }

  private static FunctionResolver simple_query_string() {
    FunctionName funcName = BuiltinFunctionName.SIMPLE_QUERY_STRING.getName();
    return new RelevanceFunctionResolver(funcName, STRUCT);
  }

  private static FunctionResolver query() {
    FunctionName funcName = BuiltinFunctionName.QUERY.getName();
    return new RelevanceFunctionResolver(funcName, STRING);
  }

  private static FunctionResolver query_string() {
    FunctionName funcName = BuiltinFunctionName.QUERY_STRING.getName();
    return new RelevanceFunctionResolver(funcName, STRUCT);
  }

  private static FunctionResolver wildcard_query(BuiltinFunctionName wildcardQuery) {
    FunctionName funcName = wildcardQuery.getName();
    return new RelevanceFunctionResolver(funcName, STRING);
  }

  public static class OpenSearchFunction extends FunctionExpression {
    private final FunctionName functionName;
    private final List<Expression> arguments;

    /**
     * Required argument constructor.
     * @param functionName name of the function
     * @param arguments a list of expressions
     */
    public OpenSearchFunction(FunctionName functionName, List<Expression> arguments) {
      super(functionName, arguments);
      this.functionName = functionName;
      this.arguments = arguments;
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      throw new UnsupportedOperationException(String.format(
          "OpenSearch defined function [%s] is only supported in WHERE and HAVING clause.",
          functionName));
    }

    @Override
    public ExprType type() {
      return ExprCoreType.BOOLEAN;
    }

    @Override
    public String toString() {
      List<String> args = arguments.stream()
          .map(arg -> String.format("%s=%s", ((NamedArgumentExpression) arg)
              .getArgName(), ((NamedArgumentExpression) arg).getValue().toString()))
          .collect(Collectors.toList());
      return String.format("%s(%s)", functionName, String.join(", ", args));
    }
  }
}
