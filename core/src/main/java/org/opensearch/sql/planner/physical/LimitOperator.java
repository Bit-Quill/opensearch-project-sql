/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.ObjectOutput;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;

/**
 * The limit operator sets a window, to and block the rows out of the window
 * and allow only the result subset within this window to the output.
 *
 * <p>The result subset is enframed from original result with {@link LimitOperator#offset}
 * as the offset and {@link LimitOperator#limit} as the size, thus the output
 * is the subset of the original result set that has indices from {index + 1} to {index + limit}.
 * Special cases might occur where the result subset has a size smaller than expected {limit},
 * it occurs when the original result set has a size smaller than {index + limit},
 * or even not greater than the offset. The latter results in an empty output.</p>
 */
@RequiredArgsConstructor
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class LimitOperator extends PhysicalPlan {
  private final PhysicalPlan input;
  private final Integer limit;
  private final Integer offset;
  private Integer count = 0;

  // Consider using a copy constructor instead -- see usage
  public LimitOperator(PhysicalPlan input, Integer limit, Integer offset, Integer count) {
    this.input = input;
    this.limit = limit;
    this.offset = offset;
    this.count = count;
  }

  @Override
  public void open() {
    super.open();

    // skip the leading rows of offset size
    while (input.hasNext() && count < offset) {
      count++;
      input.next();
    }
  }

  @Override
  public boolean hasNext() {
    var inNext = input.hasNext();
    var cond = count < offset + limit;
    return inNext && cond;
  }

  @Override
  public ExprValue next() {
    count++;
    return input.next();
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitLimit(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return ImmutableList.of(input);
  }

  @Override
  public boolean writeExternal(ObjectOutput out) throws IOException {
    PlanLoader loader = (in, engine) -> {
      var limit = in.readInt();
      var offset = in.readInt();
      var count = in.readInt();
      var inputLoader = (PlanLoader) in.readObject();
      var input = (PhysicalPlan) inputLoader.apply(in, engine);
      return new LimitOperator(input, limit, offset, count);
    };
    out.writeObject(loader);

    out.writeInt(limit);
    out.writeInt(offset);
    out.writeInt(count);
    return input.getPlanForSerialization().writeExternal(out);
  }
}
