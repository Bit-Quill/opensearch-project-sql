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
import java.io.InputStream;
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
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.planner.physical.PaginateOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;

/**
 * This class is entry point to paged requests. It is responsible to cursor serialization
 * and deserialization.
 */
@RequiredArgsConstructor
public class PaginatedPlanCache {
  public static final String CURSOR_PREFIX = "n:";

  // TODO get engine from GUICE if possible
  private final StorageEngine engine;

  public boolean canConvertToCursor(UnresolvedPlan plan) {
    return plan.accept(new CanPaginateVisitor(), null);
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
      var engine = (StorageEngine) ((CursorDeserializationStream) in).resolveObject("engine");
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
      ObjectInputStream objectInput = new CursorDeserializationStream(
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

  /**
   * This function is used in testing only, to get access to {@link CursorDeserializationStream}.
   */
  protected CursorDeserializationStream getCursorDeserializationStream(InputStream in)
      throws IOException {
    return new CursorDeserializationStream(in);
  }

  class CursorDeserializationStream extends ObjectInputStream {
    public CursorDeserializationStream(InputStream in) throws IOException {
      super(in);
    }

    @Override
    protected Object resolveObject(Object obj) throws IOException {
      return obj.equals("engine") ? engine : obj;
    }
  }
}
