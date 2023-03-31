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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.project;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.storage.StorageEngine;

@ExtendWith(MockitoExtension.class)
class ProjectOperatorTest extends PhysicalPlanTestBase {

  @Mock(serializable = true)
  private PhysicalPlan inputPlan;

  @Test
  public void project_one_field() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", 200)));
    PhysicalPlan plan = project(inputPlan, DSL.named("action", DSL.ref("action", STRING)));
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(1),
            hasItems(ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET")))));
  }

  @Test
  public void project_two_field_follow_the_project_order() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", 200)));
    PhysicalPlan plan = project(inputPlan,
        DSL.named("response", DSL.ref("response", INTEGER)),
        DSL.named("action", DSL.ref("action", STRING)));
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(1),
            hasItems(
                ExprValueUtils.tupleValue(ImmutableMap.of("response", 200, "action", "GET")))));
  }

  @Test
  public void project_keep_missing_value() {
    when(inputPlan.hasNext()).thenReturn(true, true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", 200)))
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST")));
    PhysicalPlan plan = project(inputPlan,
        DSL.named("response", DSL.ref("response", INTEGER)),
        DSL.named("action", DSL.ref("action", STRING)));
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(2),
            hasItems(
                ExprValueUtils.tupleValue(ImmutableMap.of("response", 200, "action", "GET")),
                ExprTupleValue.fromExprValueMap(ImmutableMap.of("response",
                    LITERAL_MISSING,
                    "action", stringValue("POST"))))));
  }

  @Test
  public void project_schema() {
    PhysicalPlan project = project(inputPlan,
        DSL.named("response", DSL.ref("response", INTEGER)),
        DSL.named("action", DSL.ref("action", STRING), "act"));

    assertThat(project.schema().getColumns(), contains(
        new ExecutionEngine.Schema.Column("response", null, INTEGER),
        new ExecutionEngine.Schema.Column("action", "act", STRING)
    ));
  }

  @Test
  public void project_fields_with_parse_expressions() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("response", "GET 200")));
    PhysicalPlan plan =
        project(inputPlan, ImmutableList.of(DSL.named("action", DSL.ref("action", STRING)),
                DSL.named("response", DSL.ref("response", STRING))),
            ImmutableList.of(DSL.named("action",
                DSL.regex(DSL.ref("response", STRING),
                    DSL.literal("(?<action>\\w+) (?<response>\\d+)"),
                    DSL.literal("action"))), DSL.named("response",
                DSL.regex(DSL.ref("response", STRING),
                    DSL.literal("(?<action>\\w+) (?<response>\\d+)"),
                    DSL.literal("response"))))
        );
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(1),
            hasItems(
                ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", "200")))));
  }

  @Test
  public void project_fields_with_unused_parse_expressions() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("response", "GET 200")));
    PhysicalPlan plan =
        project(inputPlan, ImmutableList.of(DSL.named("response", DSL.ref("response", STRING))),
            ImmutableList.of(DSL.named("ignored",
                DSL.regex(DSL.ref("response", STRING),
                    DSL.literal("(?<action>\\w+) (?<ignored>\\d+)"),
                    DSL.literal("ignored"))))
        );
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(1),
            hasItems(
                ExprValueUtils.tupleValue(ImmutableMap.of("response", "GET 200")))));
  }

  @Test
  public void project_fields_with_parse_expressions_and_runtime_fields() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(ImmutableMap.of("response", "GET 200", "eval_field", 1)));
    PhysicalPlan plan =
        project(inputPlan, ImmutableList.of(DSL.named("response", DSL.ref("response", STRING)),
                DSL.named("action", DSL.ref("action", STRING)),
                DSL.named("eval_field", DSL.ref("eval_field", INTEGER))),
            ImmutableList.of(DSL.named("action",
                DSL.regex(DSL.ref("response", STRING),
                    DSL.literal("(?<action>\\w+) (?<response>\\d+)"),
                    DSL.literal("action"))))
        );
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(1),
            hasItems(
                ExprValueUtils.tupleValue(
                    ImmutableMap.of("response", "GET 200", "action", "GET", "eval_field", 1)))));
  }

  @Test
  public void project_parse_missing_will_fallback() {
    when(inputPlan.hasNext()).thenReturn(true, true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", "GET 200")))
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST")));
    PhysicalPlan plan =
        project(inputPlan, ImmutableList.of(DSL.named("action", DSL.ref("action", STRING)),
                DSL.named("response", DSL.ref("response", STRING))),
            ImmutableList.of(DSL.named("action",
                DSL.regex(DSL.ref("response", STRING),
                    DSL.literal("(?<action>\\w+) (?<response>\\d+)"),
                    DSL.literal("action"))), DSL.named("response",
                DSL.regex(DSL.ref("response", STRING),
                    DSL.literal("(?<action>\\w+) (?<response>\\d+)"),
                    DSL.literal("response"))))
        );
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(2),
            hasItems(
                ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", "200")),
                ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST")))));
  }

  @Test
  @SneakyThrows
  public void writeExternal_serializes_child_plan_too() {
    var plan = mock(PhysicalPlan.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    var paginate = new ProjectOperator(plan, List.of(), List.of());
    var out = mock(ObjectOutput.class);
    doReturn(true, false).when(plan).writeExternal(out);
    assertTrue(paginate.writeExternal(out));
    assertFalse(paginate.writeExternal(out));
    verify(plan, times(2)).writeExternal(out);
    verify(plan, times(2)).getPlanForSerialization();
  }

  @Test
  @SneakyThrows
  public void writeExternal_serializes_loader() {
    var plan = mock(PhysicalPlan.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    var projects = List.of(DSL.named("action", DSL.ref("action", STRING)));
    var project = new ProjectOperator(plan, projects, List.of());
    var out = mock(ObjectOutput.class);
    doReturn(true).when(plan).writeExternal(out);

    var captor = ArgumentCaptor.forClass(Object.class);
    assertTrue(project.writeExternal(out));
    verify(out, times(2)).writeObject(captor.capture());
    verify(plan).writeExternal(out);

    assertEquals(projects, captor.getAllValues().get(1));

    var in = mock(ObjectInput.class);
    var engine = mock(StorageEngine.class);
    var childLoader = mock(SerializablePlan.PlanLoader.class);
    when(childLoader.apply(in, engine)).thenReturn(plan);
    when(in.readObject()).thenReturn(projects, childLoader);
    var loader = (SerializablePlan.PlanLoader) captor.getAllValues().get(0);
    var deserialized = loader.apply(in, engine);
    assertEquals(deserialized, project);
  }
}
