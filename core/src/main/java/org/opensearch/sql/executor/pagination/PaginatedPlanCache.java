/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import com.google.common.hash.HashCode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.planner.physical.PaginateOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;

/**
 * This class is entry point to paged requests. It is responsible to cursor serialization
 * and deserialization.
 */
public class PaginatedPlanCache implements AutoCloseable {
  public static final String CURSOR_PREFIX = "n:";

  public PaginatedPlanCache(StorageEngine storageEngine) {
    SerializationContext.engine = storageEngine;
  }

  public boolean canConvertToCursor(UnresolvedPlan plan) {
    return plan.accept(new CanPaginateVisitor(), null);
  }

  // Actually called only once on server lifetime - on shutdown, so it does nothing.
  @Override
  public void close() throws Exception {
    SerializationContext.engine = null;
  }

  @Data
  public static class SerializationContext implements Externalizable {
    private PaginateOperator plan;
    private static StorageEngine engine;
    /**
     * If exception is thrown we don't catch it, that means something really went wrong.
     * But if we can't serialize the plan, we set this flag and should return an empty cursor.
     * The only case when it could happen as of now - paging is finished and there is no scroll.
     */
    private boolean serializedSuccessfully = false;

    public SerializationContext() {
    }

    public SerializationContext(PaginateOperator plan) {
      this.plan = plan;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      serializedSuccessfully = plan.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      var loader = (SerializablePlan.PlanLoader) in.readObject();
      plan = (PaginateOperator) loader.apply(in, engine);
    }
  }

  /**
   * Converts a physical plan tree to a cursor. May cache plan related data somewhere.
   */
  public Cursor convertToCursor(PhysicalPlan plan) throws IOException {
    if (plan instanceof PaginateOperator) {
      var context = new SerializationContext((PaginateOperator) plan);
      var serialized = serialize(context);
      return context.serializedSuccessfully ? new Cursor(CURSOR_PREFIX + serialized) : Cursor.None;
    }
    return Cursor.None;
  }

  public String serialize(Serializable object) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(object);
      objectOutput.flush();

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      GZIPOutputStream gzip = new GZIPOutputStream(out) { {
        this.def.setLevel(Deflater.BEST_COMPRESSION);
      } };
      gzip.write(output.toByteArray());
      gzip.close();
      return HashCode.fromBytes(out.toByteArray()).toString();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize: " + object, e);
    }
  }

  public Serializable deserialize(String code) {
    try {
      GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(
          HashCode.fromString(code).asBytes()));
      ByteArrayInputStream input = new ByteArrayInputStream(gzip.readAllBytes());
      ObjectInputStream objectInput = new ObjectInputStream(input);
      return (Serializable) objectInput.readObject();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize object", e);
    }
  }

  /**
   * Converts a cursor to a physical plan tree.
   */
  public PhysicalPlan convertToPlan(String cursor) {
    if (!cursor.startsWith(CURSOR_PREFIX)) {
      throw new UnsupportedOperationException("Unsupported cursor");
    }
    try {
      return ((SerializationContext) deserialize(cursor.substring(CURSOR_PREFIX.length()))).getPlan();
    } catch (Exception e) {
      throw new UnsupportedOperationException("Unsupported cursor", e);
    }
  }
}
