/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import java.io.Externalizable;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.List;

import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.PaginatedPlanCache;
import org.opensearch.sql.planner.PlanNode;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.storage.split.Split;

/**
 * Physical plan.
 */
public abstract class PhysicalPlan extends SerializablePlan
    implements PlanNode<PhysicalPlan>, Iterator<ExprValue>, AutoCloseable {
  /**
   * Accept the {@link PhysicalPlanNodeVisitor}.
   *
   * @param visitor visitor.
   * @param context visitor context.
   * @param <R>     returned object type.
   * @param <C>     context type.
   * @return returned object.
   */
  public abstract <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context);

  public void open() {
    getChild().forEach(PhysicalPlan::open);
  }

  public void close() {
    getChild().forEach(PhysicalPlan::close);
  }

  public void add(Split split) {
    getChild().forEach(child -> child.add(split));
  }

  public ExecutionEngine.Schema schema() {
    throw new IllegalStateException(String.format("[BUG] schema can been only applied to "
        + "ProjectOperator, instead of %s", this.getClass().getSimpleName()));
  }

  /**
   * Returns Total hits matched the search criteria. Note: query may return less if limited.
   * {@see Settings#QUERY_SIZE_LIMIT}.
   * Any plan which adds/removes rows to the response should overwrite it to provide valid values.
   *
   * @return Total hits matched the search criteria.
   */
  public long getTotalHits() {
    return getChild().stream().mapToLong(PhysicalPlan::getTotalHits).max().orElse(0);
  }
}
