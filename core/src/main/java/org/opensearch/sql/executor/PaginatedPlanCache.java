/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.opensearch.executor.Cursor;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.planner.physical.PaginateOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.TableScanOperator;

public class PaginatedPlanCache {
  public static final String CURSOR_PREFIX = "n:";
  private final StorageEngine storageEngine;
  public static final PaginatedPlanCache None = new PaginatedPlanCache(null);

  public PaginatedPlanCache(StorageEngine storageEngine) {
    this.storageEngine = storageEngine;
    SerializationContext.engine = storageEngine;
  }

  public boolean canConvertToCursor(UnresolvedPlan plan) {
    return plan.accept(new CanPaginateVisitor(), null);
  }

  @Data
  @AllArgsConstructor
  public static class SerializationContext implements Externalizable {
    private PaginateOperator plan;
    private static StorageEngine engine;

    public SerializationContext() {
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      plan.writeExternal(out);
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
  public Cursor convertToCursor(PhysicalPlan plan) {
    if (plan instanceof PaginateOperator) {
      var context = new SerializationContext((PaginateOperator) plan);
      //plan.prepareToSerialization(context);
      //return new Cursor(CURSOR_PREFIX + serialize(new Object[] { plan, context }));
      return new Cursor(CURSOR_PREFIX + serialize(context));
    } else {
      return Cursor.None;
    }
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
   * Compress serialized query plan.
   * @param str string representing a query plan
   * @return str compressed with gzip.
   */
  @SneakyThrows
  public static String compress(String str) {
    if (str == null || str.length() == 0) {
      return null;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(out);
    gzip.write(str.getBytes());
    gzip.close();
    return HashCode.fromBytes(out.toByteArray()).toString();
  }

  /**
   * Decompresses a query plan that was compress with {@link PaginatedPlanCache#compress}.
   * @param input compressed query plan
   * @return seria
   */
  @SneakyThrows
  public static String decompress(String input) {
    if (input == null || input.length() == 0) {
      return null;
    }
    GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(
        HashCode.fromString(input).asBytes()));
    return new String(gzip.readAllBytes());
  }

  /**
   * Converts a cursor to a physical plan tree.
   */
  public PhysicalPlan convertToPlan(String cursor) {
    if (cursor.startsWith(CURSOR_PREFIX)) {
      try {
        //var data = (Object[]) deserialize(cursor.substring(CURSOR_PREFIX.length()));
        //var plan = (PhysicalPlan) data[0];
        //var context = (SerializationContext) data[1];
        //TableScanOperator scan = storageEngine.getTableScan(context.getIndexName(), context.getScrollId());

        //Class.forName("PaginateOperator").getDeclaredConstructor()

        return ((SerializationContext) deserialize(cursor.substring(CURSOR_PREFIX.length()))).getPlan();
      } catch (Exception e) {
        throw new UnsupportedOperationException("Unsupported cursor", e);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported cursor");
    }
  }
}
