/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class PaginateOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;

  @Getter
  private final int pageSize;

  /**
   * Which page is this?
   * May not be necessary in the end. Currently used to increment the "cursor counter" --
   * See usage.
   */
  @Getter
  private int pageIndex = 0;

  private int numReturned = 0;

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
  public ExprValue next() {
    numReturned += 1;
    return input.next();
  }

  public List<PhysicalPlan> getChild() {
    return List.of(input);
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return input.schema();
  }

  @Override
  public boolean writeExternal(ObjectOutput out) throws IOException {
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
    return input.getPlanForSerialization().writeExternal(out);
  }
}
