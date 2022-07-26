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

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    FunctionBuilder buildFunction
        = args -> new OpenSearchFunctions.OpenSearchFunction(functionName, args);


    if (!unresolvedSignature.getFunctionName().equals(functionName)) {
      throw new SemanticCheckException(String.format("Expected '%s' but got '%s'",
          functionName.getFunctionName(), unresolvedSignature.getFunctionName().getFunctionName()));
    }
    List<ExprType> paramTypes = unresolvedSignature.getParamTypeList();
    ExprType firstParamType = paramTypes.get(0);

    for (int i = 1; i < paramTypes.size(); i++) {
      ExprType paramType = paramTypes.get(i);
      if (!ExprCoreType.STRING.equals(paramType)) {
        throw new SemanticCheckException(
            String.format("Expect type STRING instead of %s for parameter #%d",
                paramType.typeName(), i));
      }
    }

    return Pair.of(unresolvedSignature, buildFunction);
  }
}
