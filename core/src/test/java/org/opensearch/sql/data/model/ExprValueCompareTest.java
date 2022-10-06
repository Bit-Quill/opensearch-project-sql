/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import java.time.Period;
import java.util.LinkedHashMap;

public class ExprValueCompareTest {

  @Test
  public void timeValueCompare() {
    assertEquals(0, new ExprTimeValue("18:00:00").compareTo(new ExprTimeValue("18:00:00")));
    assertEquals(1, new ExprTimeValue("19:00:00").compareTo(new ExprTimeValue("18:00:00")));
    assertEquals(-1, new ExprTimeValue("18:00:00").compareTo(new ExprTimeValue("19:00:00")));
  }

  @Test
  public void dateValueCompare() {
    assertEquals(0, new ExprDateValue("2012-08-07").compareTo(new ExprDateValue("2012-08-07")));
    assertEquals(1, new ExprDateValue("2012-08-08").compareTo(new ExprDateValue("2012-08-07")));
    assertEquals(-1, new ExprDateValue("2012-08-07").compareTo(new ExprDateValue("2012-08-08")));
  }

  @Test
  public void datetimeValueCompare() {
    assertEquals(0,
        new ExprDatetimeValue("2012-08-07 18:00:00")
            .compareTo(new ExprDatetimeValue("2012-08-07 18:00:00")));
    assertEquals(1,
        new ExprDatetimeValue("2012-08-07 19:00:00")
            .compareTo(new ExprDatetimeValue("2012-08-07 18:00:00")));
    assertEquals(-1,
        new ExprDatetimeValue("2012-08-07 18:00:00")
            .compareTo(new ExprDatetimeValue("2012-08-07 19:00:00")));
  }

  @Test
  public void timestampValueCompare() {
    assertEquals(0,
        new ExprTimestampValue("2012-08-07 18:00:00")
            .compareTo(new ExprTimestampValue("2012-08-07 18:00:00")));
    assertEquals(1,
        new ExprTimestampValue("2012-08-07 19:00:00")
            .compareTo(new ExprTimestampValue("2012-08-07 18:00:00")));
    assertEquals(-1,
        new ExprTimestampValue("2012-08-07 18:00:00")
            .compareTo(new ExprTimestampValue("2012-08-07 19:00:00")));
  }

  @Test
  public void intValueCompare() {
    assertEquals(0, new ExprIntegerValue(1).compareTo(new ExprIntegerValue(1)));
    assertEquals(1, new ExprIntegerValue(2).compareTo(new ExprIntegerValue(1)));
    assertEquals(-1, new ExprIntegerValue(1).compareTo(new ExprIntegerValue(2)));
  }

  @Test
  public void doubleValueCompare() {
    assertEquals(0, new ExprDoubleValue(1).compareTo(new ExprDoubleValue(1)));
    assertEquals(1, new ExprDoubleValue(2).compareTo(new ExprDoubleValue(1)));
    assertEquals(-1, new ExprDoubleValue(1).compareTo(new ExprDoubleValue(2)));
  }

  @Test
  public void stringValueCompare() {
    assertEquals(0, new ExprStringValue("str1").compareTo(new ExprStringValue("str1")));
    assertEquals(1, new ExprStringValue("str2").compareTo(new ExprStringValue("str1")));
    assertEquals(-1, new ExprStringValue("str1").compareTo(new ExprStringValue("str2")));
  }

  @Test
  public void intervalValueCompare() {
    assertEquals(0, new ExprIntervalValue(Period.ofDays(1))
        .compareTo(new ExprIntervalValue(Period.ofDays(1))));
    assertEquals(1, new ExprIntervalValue(Period.ofDays(2))
        .compareTo(new ExprIntervalValue(Period.ofDays(1))));
    assertEquals(-1, new ExprIntervalValue(Period.ofDays(1))
        .compareTo(new ExprIntervalValue(Period.ofDays(2))));
  }

  @Test
  public void collectionValueCompare() {
    assertEquals(0, new ExprCollectionValue(ImmutableList.of())
        .compareTo(new ExprCollectionValue(ImmutableList.of())));
    assertEquals(0, new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(1)))
        .compareTo(new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(1)))));
    assertEquals(0, new ExprCollectionValue(ImmutableList.of(
            new ExprIntegerValue(1), new ExprIntegerValue(2)))
        .compareTo(new ExprCollectionValue(ImmutableList.of(
            new ExprIntegerValue(1), new ExprIntegerValue(2)))));
    assertEquals(1, new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(2)))
        .compareTo(new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(1)))));
    assertEquals(1, new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(1)))
        .compareTo(new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(2)))));
    assertEquals(1, new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(1)))
        .compareTo(new ExprCollectionValue(ImmutableList.of(
            new ExprIntegerValue(1), new ExprIntegerValue(1)))));
    assertEquals(1, new ExprCollectionValue(ImmutableList.of(
            new ExprIntegerValue(1), new ExprIntegerValue(2)))
        .compareTo(new ExprCollectionValue(ImmutableList.of(
            new ExprIntegerValue(1), new ExprIntegerValue(1)))));
    assertEquals(1, new ExprCollectionValue(ImmutableList.of(
            new ExprIntegerValue(1), new ExprIntegerValue(2)))
        .compareTo(new ExprCollectionValue(ImmutableList.of(
            new ExprIntegerValue(2), new ExprIntegerValue(1)))));
  }

  @Test
  public void tupleValueCompare() {
    assertEquals(0, new ExprTupleValue(new LinkedHashMap<>())
        .compareTo(new ExprTupleValue(new LinkedHashMap<>())));
    assertEquals(0, new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(1))))
        .compareTo(new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(1))))));
    assertEquals(0, new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(1), "2", new ExprIntegerValue(2))))
        .compareTo(new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(1), "2", new ExprIntegerValue(2))))));
    assertEquals(1, new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(2))))
        .compareTo(new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(1))))));
    assertEquals(1, new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(1))))
        .compareTo(new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(2))))));
    assertEquals(1, new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(1))))
        .compareTo(new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("2", new ExprIntegerValue(1))))));
    assertEquals(1, new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("2", new ExprIntegerValue(1))))
        .compareTo(new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(1))))));
    assertEquals(1, new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(1), "2", new ExprIntegerValue(1))))
        .compareTo(new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(1))))));
    assertEquals(1, new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("1", new ExprIntegerValue(1), "2", new ExprIntegerValue(1))))
        .compareTo(new ExprTupleValue(new LinkedHashMap<>(
            ImmutableMap.of("2", new ExprIntegerValue(1), "1", new ExprIntegerValue(1))))));
  }

  @Test
  public void missingCompareToMethodShouldNotBeenCalledDirectly() {
    IllegalStateException exception = assertThrows(IllegalStateException.class,
        () -> LITERAL_MISSING.compareTo(LITERAL_FALSE));
    assertEquals("[BUG] Unreachable, Comparing with NULL or MISSING is undefined",
        exception.getMessage());

    exception = assertThrows(IllegalStateException.class,
        () -> LITERAL_FALSE.compareTo(LITERAL_MISSING));
    assertEquals("[BUG] Unreachable, Comparing with NULL or MISSING is undefined",
        exception.getMessage());

    exception = assertThrows(IllegalStateException.class,
        () -> ExprMissingValue.of().compare(LITERAL_MISSING));
    assertEquals("[BUG] Unreachable, Comparing with MISSING is undefined",
        exception.getMessage());
  }

  @Test
  public void nullCompareToMethodShouldNotBeenCalledDirectly() {
    IllegalStateException exception = assertThrows(IllegalStateException.class,
        () -> LITERAL_NULL.compareTo(LITERAL_FALSE));
    assertEquals("[BUG] Unreachable, Comparing with NULL or MISSING is undefined",
        exception.getMessage());

    exception = assertThrows(IllegalStateException.class,
        () -> LITERAL_FALSE.compareTo(LITERAL_NULL));
    assertEquals("[BUG] Unreachable, Comparing with NULL or MISSING is undefined",
        exception.getMessage());

    exception = assertThrows(IllegalStateException.class,
        () -> ExprNullValue.of().compare(LITERAL_MISSING));
    assertEquals("[BUG] Unreachable, Comparing with NULL is undefined",
        exception.getMessage());
  }
}
