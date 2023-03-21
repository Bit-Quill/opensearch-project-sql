/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.PaginatedPlanCache;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.storage.StorageEngine;

@EqualsAndHashCode(callSuper = false)
public class PaginateOperator extends PhysicalPlan {
  @Getter
  private PhysicalPlan input;

  @Getter
  private int pageSize;

  /**
   * Which page is this?
   * May not be necessary in the end. Currently used to increment the "cursor counter" --
   * See usage.
   */
  @Getter
  private int pageIndex = 0;

  private int numReturned = 0;

  public PaginateOperator() {
    int a = 5;
    // TODO validate that called only from deserializer
  }

  /**
   * Page given physical plan, with pageSize elements per page, starting with the first page.
   */
  public PaginateOperator(PhysicalPlan input, int pageSize) {
    this.pageSize = pageSize;
    this.input = input;
  }

  /**
   * Page given physical plan, with pageSize elements per page, starting with the given page.
   */
  public PaginateOperator(PhysicalPlan input, int pageSize, int pageIndex) {
    this.pageSize = pageSize;
    this.input = input;
    this.pageIndex = pageIndex;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitPaginate(this, context);
  }

  @Override
  public boolean hasNext() {
    return numReturned < pageSize && input.hasNext();
  }

  @Override
  public void open() {
    super.open();
    // TODO numReturned set to 0 for each new object. Do plans support re-opening?
    numReturned = 0;
  }

  @Override
  public ExprValue next() {
    numReturned += 1;
    return input.next();
  }

  public List<PhysicalPlan> getChild() {
    return List.of(input);
  }

  @Override
  public ExecutionEngine.Schema schema() {
    // TODO remove assert or do in constructor
    if (!(input instanceof ProjectOperator)) {
      throw new UnsupportedOperationException();
    }
    return input.schema();
  }

//  @Override
//  public void prepareToSerialization(PaginatedPlanCache.SerializationContext context) {
//    pageIndex++;
//  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    PlanLoader loader = (in, engine) -> {
      var pageSize = in.readInt();
      var pageIndex = in.readInt();
      var inputLoader = (PlanLoader) in.readObject();
      var input = (PhysicalPlan) inputLoader.apply(in, engine);
      return new PaginateOperator(input, pageSize, pageIndex);
    };
    out.writeObject(loader);

    out.writeInt(pageSize);
    out.writeInt(pageIndex + 1);
    input.getPlanForSerialization().writeExternal(out);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // nothing, everything done by loader
  }
/*
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    var loader = (PlanLoader) in.readObject();
    this = loader.apply(in, engine);

    input = (PhysicalPlan) in.readObject();
    pageSize = in.readInt();
    pageIndex = in.readInt();
  }*/
}
