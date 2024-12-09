/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;

public class RareTopNOperatorTest extends PhysicalPlanTestBase {

  @Test
  public void rare_without_group() {
    PhysicalPlan plan =
        new RareTopNOperator(
            new TestScan(),
            CommandType.RARE,
            List.of(DSL.ref("action", ExprCoreType.STRING)),
            List.of());
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST")),
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET"))));
  }

  @Test
  public void rare_with_group() {
    PhysicalPlan plan =
        new RareTopNOperator(
            new TestScan(),
            CommandType.RARE,
            List.of(DSL.ref("response", ExprCoreType.INTEGER)),
            List.of(DSL.ref("action", ExprCoreType.STRING)));
    List<ExprValue> result = execute(plan);
    assertEquals(4, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST", "response", 200)),
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST", "response", 500)),
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", 404)),
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", 200))));
  }

  @Test
  public void top_without_group() {
    PhysicalPlan plan =
        new RareTopNOperator(
            new TestScan(),
            CommandType.TOP,
            List.of(DSL.ref("action", ExprCoreType.STRING)),
            List.of());
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET")),
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST"))));
  }

  @Test
  public void top_n_without_group() {
    PhysicalPlan plan =
        new RareTopNOperator(
            new TestScan(),
            CommandType.TOP,
            1,
            List.of(DSL.ref("action", ExprCoreType.STRING)),
            List.of());
    List<ExprValue> result = execute(plan);
    assertEquals(1, result.size());
    assertThat(
        result, containsInAnyOrder(ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET"))));
  }

  @Test
  public void top_n_with_group() {
    PhysicalPlan plan =
        new RareTopNOperator(
            new TestScan(),
            CommandType.TOP,
            1,
            List.of(DSL.ref("response", ExprCoreType.INTEGER)),
            List.of(DSL.ref("action", ExprCoreType.STRING)));
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST", "response", 200)),
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", 200))));
  }
}
