/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.project;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.storage.StorageEngine;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class PaginateOperatorTest extends PhysicalPlanTestBase {

  @Test
  public void accept() {
    var visitor = new PhysicalPlanNodeVisitor<Integer, Object>() {};
    assertNull(new PaginateOperator(null, 42).accept(visitor, null));
  }

  @Test
  public void hasNext_a_page() {
    var plan = mock(PhysicalPlan.class);
    when(plan.hasNext()).thenReturn(true);
    when(plan.next()).thenReturn(new ExprIntegerValue(42)).thenReturn(null);
    var paginate = new PaginateOperator(plan, 1, 1);
    assertTrue(paginate.hasNext());
    assertEquals(42, paginate.next().integerValue());
    paginate.next();
    assertFalse(paginate.hasNext());
    assertNull(paginate.next());
  }

  @Test
  public void hasNext_no_more_entries() {
    var plan = mock(PhysicalPlan.class);
    when(plan.hasNext()).thenReturn(false);
    var paginate = new PaginateOperator(plan, 1, 1);
    assertFalse(paginate.hasNext());
  }

  @Test
  public void getChild() {
    var plan = mock(PhysicalPlan.class);
    var paginate = new PaginateOperator(plan, 1);
    assertSame(plan, paginate.getChild().get(0));
  }

  @Test
  public void open() {
    var plan = mock(PhysicalPlan.class);
    doNothing().when(plan).open();
    new PaginateOperator(plan, 1).open();
    verify(plan, times(1)).open();
  }

  @Test
  public void schema() {
    PhysicalPlan project = project(null,
        DSL.named("response", DSL.ref("response", INTEGER)),
        DSL.named("action", DSL.ref("action", STRING), "act"));
    assertEquals(project.schema(), new PaginateOperator(project, 42).schema());
  }

  @Test
  public void schema_assert() {
    var plan = mock(PhysicalPlan.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    assertThrows(Throwable.class, () -> new PaginateOperator(plan, 42).schema());
  }

  @Test
  @SneakyThrows
  public void writeExternal_serializes_child_plan_too() {
    var plan = mock(PhysicalPlan.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    var paginate = new PaginateOperator(plan, 1, 1);
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
    var paginate = new PaginateOperator(plan, 42, 1);
    var out = mock(ObjectOutput.class);
    doReturn(true).when(plan).writeExternal(out);

    var loaderCaptor = ArgumentCaptor.forClass(SerializablePlan.PlanLoader.class);
    var intCaptor = ArgumentCaptor.forClass(Integer.class);
    assertTrue(paginate.writeExternal(out));
    verify(out).writeObject(loaderCaptor.capture());
    verify(out, times(2)).writeInt(intCaptor.capture());
    verify(plan).writeExternal(out);

    var pageSize = intCaptor.getAllValues().get(0);
    var pageIndex = intCaptor.getAllValues().get(1);
    assertEquals(42, pageSize);
    assertEquals(2, pageIndex);

    var in = mock(ObjectInput.class);
    var engine = mock(StorageEngine.class);
    when(in.readInt()).thenReturn(pageSize, pageIndex);
    var childLoader = mock(SerializablePlan.PlanLoader.class);
    when(childLoader.apply(in, engine)).thenReturn(plan);
    when(in.readObject()).thenReturn(childLoader);
    var loader = loaderCaptor.getValue();
    var deserialized = loader.apply(in, engine);
    // can't compare `deserialized` with `paginate`, because page index was incremented
    assertEquals(deserialized, new PaginateOperator(plan, 42, 2));
  }
}
