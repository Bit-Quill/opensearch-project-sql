/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;

@ExtendWith(MockitoExtension.class)
class HighlightOperatorTest extends PhysicalPlanTestBase {
  @Mock
  private PhysicalPlan inputPlan;

  @Test
  public void do_nothing_with_none_tuple_value() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(ExprValueUtils.integerValue(1));
    PhysicalPlan plan = new HighlightOperator(inputPlan, DSL.ref("reference", STRING));
    List<ExprValue> result = execute(plan);
    assertThat(result, allOf(iterableWithSize(1), hasItems(ExprValueUtils.integerValue(1))));
  }

  @Test
  public void highlight_one_field() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(
            tupleValue(ImmutableMap.of(
                "_highlight.region", "us-east-1", "action", "GET", "response", 200)))
        .thenReturn(
            tupleValue(ImmutableMap.of(
                "_highlight.region", "us-east-1", "action", "POST", "response", 200)))
        .thenReturn(
            tupleValue(ImmutableMap.of(
                "_highlight.region", "us-east-1", "action", "PUT", "response", 200)));

    assertThat(
        execute(new HighlightOperator(inputPlan, DSL.ref("region", STRING))),
        contains(
            tupleValue(ImmutableMap.of(
                "_highlight.region", "us-east-1", "action", "GET",
                "response", 200, "highlight(region)", "us-east-1")),
            tupleValue(ImmutableMap.of(
                "_highlight.region", "us-east-1", "action", "POST",
                "response", 200, "highlight(region)", "us-east-1")),
            tupleValue(ImmutableMap.of(
                "_highlight.region", "us-east-1", "action", "PUT",
                "response", 200, "highlight(region)", "us-east-1"))
        ));
  }
}
