/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.storage.split.Split;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PhysicalPlanTest {
  @Mock
  Split split;

  @Mock
  PhysicalPlan child;

  private PhysicalPlan testPlan = new PhysicalPlan() {
    @Override
    public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ExprValue next() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<PhysicalPlan> getChild() {
      return List.of(child);
    }
  };

  @Test
  void add_split_to_child_by_default() {
    testPlan.add(split);
    verify(child).add(split);
  }

  @Test
  void get_total_hits_from_child() {
    var plan = mock(PhysicalPlan.class);
    when(child.getTotalHits()).thenReturn(42L);
    when(plan.getChild()).thenReturn(List.of(child));
    when(plan.getTotalHits()).then(CALLS_REAL_METHODS);
    assertEquals(42, plan.getTotalHits());
    verify(child).getTotalHits();
  }

  @Test
  void get_total_hits_uses_default_value() {
    var plan = mock(PhysicalPlan.class);
    when(plan.getTotalHits()).then(CALLS_REAL_METHODS);
    assertEquals(0, plan.getTotalHits());
  }

  @Test
  void toCursor() {
    var plan = mock(PhysicalPlan.class);
    when(plan.prepareToSerialization()).then(CALLS_REAL_METHODS);
    assertTrue(assertThrows(IllegalStateException.class, plan::prepareToSerialization)
        .getMessage().contains("is not compatible with cursor feature"));
  }

  @Test
  void createSection() {
    var plan = mock(PhysicalPlan.class);
    when(plan.createSection(anyString(), any())).then(CALLS_REAL_METHODS);
    assertEquals("(plan,one,two)", plan.createSection("plan", "one", "two"));
  }
}
