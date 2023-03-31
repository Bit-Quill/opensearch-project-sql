/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import com.google.common.hash.HashCode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import org.opensearch.sql.ast.tree.UnresolvedPlan;
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

  /**
   * See {@link SerializationContext#engine}.
   */
  public PaginatedPlanCache(StorageEngine engine) {
    SerializationContext.engine = engine;
  }

  public boolean canConvertToCursor(UnresolvedPlan plan) {
    return plan.accept(new CanPaginateVisitor(), null);
  }

  /**
   * Actually this happens only on OpenSearch node shutdown, because lifetime of PaginatedPlanCache
   * is equals to SQL plugin lifetime.
   */
  @Override
  public void close() throws Exception {
    SerializationContext.engine = null;
  }

  /**
   * An auxiliary class, which provides an entry point for serialization and deserialization of
   * the plan tree. It doesn't serialize itself, it calls {@link SerializablePlan#writeExternal}
   * of the given plan. For deserialization, it loads a {@link SerializablePlan.PlanLoader} and
   * invokes it.
   */
  @Data
  public static class SerializationContext implements Externalizable {
    private PaginateOperator plan;
    // TODO get engine from GUICE if possible
    /**
     * We have to make {@link PaginatedPlanCache.SerializationContext} a non-static class, we can
     * avoid setting engine statically, but in that case deserialization fails with exception
     * 'no default (no-arg) constructor', even if it has one. This is caused because default
     * constructor ofr a non-static class isn't available from outside (from serialization engine).
     * The engine is not being serialized, but it is required for deserialization.
     * See usages of {@link SerializablePlan.PlanLoader}, especially in `OpenSearchPagedIndexScan`.
     */
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
   * Converts a physical plan tree to a cursor.
   */
  public Cursor convertToCursor(PhysicalPlan plan) {
    if (plan instanceof PaginateOperator) {
      var context = new SerializationContext((PaginateOperator) plan);
      var serialized = serialize(context);
      return context.serializedSuccessfully ? new Cursor(CURSOR_PREFIX + serialized) : Cursor.None;
    }
    return Cursor.None;
  }

  /**
   * Serializes and compresses the object.
   * @param object The object.
   * @return Encoded binary data.
   */
  protected String serialize(Serializable object) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(object);
      objectOutput.flush();

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      // GZIP provides 35-45%, lzma from apache commons-compress has few % better compression
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

  /**
   * Decompresses and deserializes the binary data.
   * @param code Encoded binary data.
   * @return An object.
   */
  protected Serializable deserialize(String code) {
    try {
      GZIPInputStream gzip = new GZIPInputStream(
          new ByteArrayInputStream(HashCode.fromString(code).asBytes()));
      ObjectInputStream objectInput = new ObjectInputStream(
          new ByteArrayInputStream(gzip.readAllBytes()));
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
      return ((SerializationContext) deserialize(cursor.substring(CURSOR_PREFIX.length())))
          .getPlan();
    } catch (Exception e) {
      throw new UnsupportedOperationException("Unsupported cursor", e);
    }
  }
}
