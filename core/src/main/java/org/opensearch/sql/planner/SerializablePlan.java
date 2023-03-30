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
 *
 * This class can't implement Externalizable, because deserialization of Externalizable objects
 * works the following way:
 * 1. A new object created with no-arg constructor (no PhysicalPlan has it)
 * 2. Object loads data from the stream.
 * Externalizable interface was split into two pars: serialization is kept with
 * {@link #writeExternal}, but deserialization is provided by {@link PlanLoader}.
 */
public abstract class SerializablePlan {

  // Copied from Externalizable
  /**
   *  Each plan which supports serialization should dump itself into the stream and go recursive.
   *  It is good to create and dump a {@link PlanLoader} here as well. See usage samples.
   *  <pre>{@code
   *  out.writeSomething(data);
   *  for (var plan : getChild()) {
   *    plan.getPlanForSerialization().writeExternal(out);
   *  }
   *  }</pre>
  */
  public boolean writeExternal(ObjectOutput out) throws IOException {
    throw new NotImplementedException();
  }


  /**
   * Override to return child or delegated plan, so parent plan should skip this one
   * for serialization, but it should try to serialize grandchild plan.
   *
   * Imagine plan structure like this
   *    A         -> this
   *    `- B      -> child
   *      `- C    -> this
   * In that case only plans A and C should be attempted to serialize.
   * It is needed to skip a `ResourceMonitorPlan` instance only, actually.
   * @return Next plan for serialization.
   */
  public SerializablePlan getPlanForSerialization() {
    return this;
  }

  /**
   * Each plan should serialize an instance of this function.
   * The function deserializes and creates a new instance of that plan type.
   * A loader of a plan X could be defined only in scope of X, because only X
   * knows how to create a new X.
   * Deserialization should match with serialization given in {@link #writeExternal}.
   */
  @FunctionalInterface
  public interface PlanLoader extends Serializable {
     SerializablePlan apply(ObjectInput in, StorageEngine engine)
         throws IOException, ClassNotFoundException;
  }
}
