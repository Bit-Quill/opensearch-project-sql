package org.opensearch.sql.expression.function;

import org.apache.commons.lang3.tuple.Pair;

/**
 * An interface for any class that can provide a {@ref FunctionBuilder}
 * given a {@ref FunctionSignature}.
 */
public interface FunctionResolver {
  Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature);

  FunctionName getFunctionName();
}
