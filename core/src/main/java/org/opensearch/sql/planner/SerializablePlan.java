/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.sql.storage.StorageEngine;

/**
 * All instances of PhysicalPlan which needs to be serialized (in cursor feature) should
 * override all given here methods.
 */
public abstract class SerializablePlan {

  // Copied from Externalizable
  public boolean writeExternal(ObjectOutput out) throws IOException {
    throw new NotImplementedException(String.format("%s is not serializable",
        getClass().getSimpleName()));
    /* Each plan which supports serialization should dump itself into the stream and go recursive.
    TODO update comment
    out.writeSomething(data);
    for (var plan : getChild()) {
      plan.writeExternal(out.getPlanForSerialization());
    }
    */
  }


  /**
   * TODO update comment
   * Override to return null, so parent plan should skip this child for serialization, but
   * it should try to serialize grandchild plan.
   *
   * Imagine plan structure like this
   *    A         -> false
   *    `- B      -> true
   *      `- C    -> false
   * In that case only plans A and C should be attempted to serialize.
   * It is needed to skip a `ResourceMonitorPlan` instance only, actually.
   */
  public SerializablePlan getPlanForSerialization() {
    return this;
  }

  @FunctionalInterface
  public interface PlanLoader extends Serializable {
     SerializablePlan apply(ObjectInput in, StorageEngine engine)
         throws IOException, ClassNotFoundException;
  }
}
