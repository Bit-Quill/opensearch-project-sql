/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.serialization;

import java.util.Base64;
import org.opensearch.sql.expression.Expression;


/**
 * Default serializer that (de-)serialize expressions by JDK serialization.
 */
public class DefaultExpressionSerializer implements ExpressionSerializer {

  NoEncodeExpressionSerializer noEncodeSerializer = new NoEncodeExpressionSerializer();

  @Override
  public String serialize(Expression expr) {
    return Base64.getEncoder().encodeToString(noEncodeSerializer.serialize(expr));
  }

  @Override
  public Expression deserialize(String code) {
    try {
      return noEncodeSerializer.deserialize(Base64.getDecoder().decode(code));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize expression code: " + code, e);
    }
  }

}
