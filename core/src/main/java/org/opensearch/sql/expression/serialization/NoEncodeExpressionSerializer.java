/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.opensearch.sql.expression.Expression;

public class NoEncodeExpressionSerializer {

  /**
   * Serialize an expression into a byte array.
   */
  public byte[] serialize(Expression expr) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(expr);
      objectOutput.flush();
      return output.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize expression: " + expr, e);
    }
  }

  /**
   * Create an expression from a serialized byte array.
   */
  public Expression deserialize(byte[] code) {
    try {
      ByteArrayInputStream input = new ByteArrayInputStream(code);
      ObjectInputStream objectInput = new ObjectInputStream(input);
      return (Expression) objectInput.readObject();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize expression code: " + code, e);
    }
  }

}
