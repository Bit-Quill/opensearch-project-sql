/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.time.Instant;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.planner.physical.PaginateOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.TableScanOperator;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class PaginatedPlanCacheTest {

  StorageEngine storageEngine;

  PaginatedPlanCache planCache;

  @BeforeEach
  void setUp() {
    storageEngine = mock(StorageEngine.class);
    when(storageEngine.getTableScan(anyString(), anyString()))
        .thenReturn(new MockedTableScanOperator());
    planCache = new PaginatedPlanCache(storageEngine);
  }

  @Test
  void canConvertToCursor_relation() {
    assertTrue(planCache.canConvertToCursor(AstDSL.relation("Table")));
  }

  @Test
  void canConvertToCursor_project_allFields_relation() {
    var unresolvedPlan = AstDSL.project(AstDSL.relation("table"), AstDSL.allFields());
    assertTrue(planCache.canConvertToCursor(unresolvedPlan));
  }

  @Test
  void canConvertToCursor_project_some_fields_relation() {
    var unresolvedPlan = AstDSL.project(AstDSL.relation("table"), AstDSL.field("rando"));
    Assertions.assertFalse(planCache.canConvertToCursor(unresolvedPlan));
  }

  @ParameterizedTest
  @ValueSource(strings = {"pewpew", "asdkfhashdfjkgakgfwuigfaijkb", "ajdhfgajklghadfjkhgjkadhgad"
      + "kadfhgadhjgfjklahdgqheygvskjfbvgsdklgfuirehiluANUIfgauighbahfuasdlhfnhaughsdlfhaughaggf"
      + "and_some_other_funny_stuff_which_could_be_generated_while_sleeping_on_the_keyboard"})
  void serialize_deserialize_str(String input) {
    var compressed = serialize(input);
    assertEquals(input, deserialize(compressed));
    if (input.length() > 200) {
      // Compression of short strings isn't profitable, because encoding into string and gzip
      // headers add more bytes than input string has.
      assertTrue(compressed.length() < input.length());
    }
  }

  public static class SerializableTestClass implements Serializable {
    public int field;

    @Override
    public boolean equals(Object obj) {
      return field == ((SerializableTestClass) obj).field;
    }
  }

  // Can't serialize private classes because they are not accessible
  private class NotSerializableTestClass implements Serializable {
    public int field;

    @Override
    public boolean equals(Object obj) {
      return field == ((SerializableTestClass) obj).field;
    }
  }

  @Test
  void serialize_deserialize_obj() {
    var obj = new SerializableTestClass();
    obj.field = 42;
    assertEquals(obj, deserialize(serialize(obj)));
    assertNotSame(obj, deserialize(serialize(obj)));
  }

  @Test
  void serialize_throws() {
    assertThrows(Throwable.class, () -> serialize(new NotSerializableTestClass()));
  }

  @Test
  void deserialize_throws() {
    assertAll(
        // from gzip - damaged header
        () -> assertThrows(Throwable.class, () -> deserialize("00")),
        // from HashCode::fromString
        () -> assertThrows(Throwable.class, () -> deserialize("000"))
    );
  }

  @Test
  @SneakyThrows
  void convertToCursor_returns_no_cursor_if_cant_serialize() {
    var plan = mock(PaginateOperator.class);
    doReturn(false, true).when(plan).writeExternal(any());
    assertAll(
        () -> assertEquals(Cursor.None, planCache.convertToCursor(plan)),
        () -> assertNotEquals(Cursor.None, planCache.convertToCursor(plan))
    );
  }

  @Test
  @SneakyThrows
  void convertToCursor_returns_no_cursor_if_plan_is_not_paginate() {
    var plan = mock(PhysicalPlan.class);
    assertEquals(Cursor.None, planCache.convertToCursor(plan));
  }

  @Test
  void convertToPlan_throws_cursor_has_no_prefix() {
    assertThrows(UnsupportedOperationException.class, () ->
        planCache.convertToPlan("abc"));
  }

  @Test
  void convertToPlan_throws_if_failed_to_deserialize() {
    assertThrows(UnsupportedOperationException.class, () ->
        planCache.convertToPlan("n:" + serialize(mock(Serializable.class))));
  }

  @Test
  void serialize_and_deserialize() {
    var plan = new TestPaginateOperator(42);
    var context = new PaginatedPlanCache.SerializationContext(plan);
    var roundTripPlan = ((PaginatedPlanCache.SerializationContext)
        planCache.deserialize(planCache.serialize(context))).getPlan();
    assertEquals(roundTripPlan, plan);
    assertNotSame(roundTripPlan, plan);
  }

  @Test
  void convertToCursor_and_convertToPlan() {
    var plan = new TestPaginateOperator(42);
    var roundTripPlan = (TestPaginateOperator)
        planCache.convertToPlan(planCache.convertToCursor(plan).toString());
    assertEquals(roundTripPlan, plan);
    assertNotSame(roundTripPlan, plan);
  }

  @Test
  @SneakyThrows
  void unset_engine_on_close() {
    var plan = mock(PaginateOperator.class);
    when(plan.writeExternal(any())).then((Answer<Boolean>) invocation -> {
      SerializablePlan.PlanLoader loader = (in, engine) -> {
        assertNull(engine);
        return mock(PaginateOperator.class);
      };
      ObjectOutput out = invocation.getArgument(0);
      out.writeObject(loader);
      return true;
    });
    var context = new PaginatedPlanCache.SerializationContext(plan);
    planCache.close();
    planCache.deserialize(planCache.serialize(context));
  }

  // Helpers and auxiliary classes section below

  public static class TestPaginateOperator extends PaginateOperator {
    private final int field;

    public TestPaginateOperator(int value) {
      super(null, 0, 0);
      field = value;
    }

    @Override
    public boolean writeExternal(ObjectOutput out) throws IOException {
      PlanLoader loader = (in, engine) -> new TestPaginateOperator(in.readInt());

      out.writeObject(loader);
      out.writeInt(field);
      return true;
    }

    @Override
    public boolean equals(Object o) {
      return field == ((TestPaginateOperator) o).field;
    }
  }

  private static class MockedTableScanOperator extends TableScanOperator {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public ExprValue next() {
      return null;
    }

    @Override
    public String explain() {
      return null;
    }
  }

  private String serialize(Serializable input) {
    return planCache.serialize(input);
  }

  private Serializable deserialize(String input) {
    return planCache.deserialize(input);
  }
}
