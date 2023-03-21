/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner;

import org.opensearch.sql.storage.StorageEngine;

import java.io.Externalizable;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.function.BiFunction;

/**
 * All instances of PhysicalPlan which needs to be serialized (in cursor feature) should
 * have a public no-arg constructor and override all given here methods.
 */
public abstract class SerializablePlan implements Externalizable {

//  /**
//   * Prep
//   * Each plan which supports serialization should override this method, to do at least nothing.
//   */
//  public void prepareToSerialization(PaginatedPlanCache.SerializationContext context) {
//    throw new IllegalStateException(String.format("%s is not compatible with cursor feature",
//        this.getClass().getSimpleName()));
//    /* Non default implementation should be like reverse visitor
//    context.setSomething(data);
//    getChild().forEach(plan -> plan.prepareToSerialization(context));
//    */
//  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    throw new NotSerializableException();
    /* Each plan which supports serialization should dump itself into the stream and go recursive.
    out.writeSomething(data);
    for (var plan : getChild()) {
      plan.writeExternal(out.getPlanForSerialization());
    }
    */
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    throw new NotSerializableException();
    /* Each plan which supports serialization should load itself from the stream and go recursive.
    this.data = in.readSomething();
    for (var plan : getChild()) {
      plan.readExternal(in);
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
