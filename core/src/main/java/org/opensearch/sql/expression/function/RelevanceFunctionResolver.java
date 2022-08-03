package org.opensearch.sql.expression.function;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;

@RequiredArgsConstructor
public class RelevanceFunctionResolver
    implements FunctionResolver {

  @Getter
  private final FunctionName functionName;

  @Getter
  private final ExprType declaredFirstParamType;

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    if (!unresolvedSignature.getFunctionName().equals(functionName)) {
      throw new SemanticCheckException(String.format("Expected '%s' but got '%s'",
          functionName.getFunctionName(), unresolvedSignature.getFunctionName().getFunctionName()));
    }
    List<ExprType> paramTypes = unresolvedSignature.getParamTypeList();
    ExprType providedFirstParamType = paramTypes.get(0);

    if (!declaredFirstParamType.equals(providedFirstParamType)) {
      throw new SemanticCheckException(
          getWrongParameterErrorMessage(0, providedFirstParamType, declaredFirstParamType));
    }

    for (int i = 1; i < paramTypes.size(); i++) {
      ExprType paramType = paramTypes.get(i);
      if (!ExprCoreType.STRING.equals(paramType)) {
        throw new SemanticCheckException(
            getWrongParameterErrorMessage(i, paramType, ExprCoreType.STRING));
      }
    }

    FunctionBuilder buildFunction =
        args -> new OpenSearchFunctions.OpenSearchFunction(functionName, args);
    return Pair.of(unresolvedSignature, buildFunction);
  }

  private String getWrongParameterErrorMessage(int i, ExprType paramType, ExprType expectedType) {
    return String.format("Expected type %s instead of %s for parameter #%d",
        expectedType.typeName(), paramType.typeName(), i + 1);
  }
}
