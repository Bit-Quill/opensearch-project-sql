/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.ReferenceExpression;

@ExtendWith(MockitoExtension.class)
class UnnestOperatorTest extends PhysicalPlanTestBase {
  @Mock
  private PhysicalPlan inputPlan;

  private final ExprValue testData = tupleValue(
      ImmutableMap.of(
          "message",
          collectionValue(
              ImmutableList.of(
                  ImmutableMap.of("info", "a"),
                  ImmutableMap.of("info", "b"),
                  ImmutableMap.of("info", "c")
              )
          ),
          "comment",
          collectionValue(
              ImmutableList.of(
                  ImmutableMap.of("data", "1"),
                  ImmutableMap.of("data", "2"),
                  ImmutableMap.of("data", "3")
              )
          )
      )
  );

  private final ExprValue nonNestedTestData = tupleValue(
      ImmutableMap.of(
          "message", "val"
      )
  );

  private final ExprValue missingTupleData = tupleValue(
      ImmutableMap.of(
          "tuple",
          tupleValue(
              ImmutableMap.of("missing", "value")
          )
      )
  );

  private final ExprValue missingArrayData = tupleValue(
      ImmutableMap.of(
          "missing",
          collectionValue(
              List.of("value")
          )
      )
  );

  @Test
  public void nested_one_nested_field() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(testData);

    Set<String> fields = Set.of("message.info");
    assertThat(
        execute(new UnnestOperator(inputPlan, fields)),
        contains(
            tupleValue(ImmutableMap.of("message.info", "a", "comment.data", "1")),
            tupleValue(ImmutableMap.of("message.info", "b", "comment.data", "1")),
            tupleValue(ImmutableMap.of("message.info", "c", "comment.data", "1"))
        )
    );
  }

  @Test
  public void nested_two_nested_field() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(testData);

    List<Map<String, ReferenceExpression>> fields =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)),
            Map.of(
                "field", new ReferenceExpression("comment.data", STRING),
                "path", new ReferenceExpression("comment", STRING))
        );

    assertThat(
        execute(new UnnestOperator(inputPlan, fields)),
        contains(
            tupleValue(ImmutableMap.of("message.info", "a", "comment.data", "1")),
            tupleValue(ImmutableMap.of("message.info", "a", "comment.data", "2")),
            tupleValue(ImmutableMap.of("message.info", "a", "comment.data", "3")),
            tupleValue(ImmutableMap.of("message.info", "b", "comment.data", "1")),
            tupleValue(ImmutableMap.of("message.info", "b", "comment.data", "2")),
            tupleValue(ImmutableMap.of("message.info", "b", "comment.data", "3")),
            tupleValue(ImmutableMap.of("message.info", "c", "comment.data", "1")),
            tupleValue(ImmutableMap.of("message.info", "c", "comment.data", "2")),
            tupleValue(ImmutableMap.of("message.info", "c", "comment.data", "3"))
        )
    );
  }

  @Test
  public void non_nested_field_tests() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(nonNestedTestData);

    Set<String> fields = Set.of("message");
    assertThat(
        execute(new UnnestOperator(inputPlan, fields)),
        contains(
            tupleValue(ImmutableMap.of("message", "val"))
        )
    );
  }

  @Test
  public void nested_missing_tuple_field() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(missingTupleData);
    Set<String> fields = Set.of("message.invalid");
    assertTrue(
        execute(new UnnestOperator(inputPlan, fields))
        .get(0)
        .tupleValue()
        .size() == 0
    );
  }

  @Test
  public void nested_missing_array_field() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(missingArrayData);
    Set<String> fields = Set.of("missing.data");
    assertTrue(
        execute(new UnnestOperator(inputPlan, fields))
            .get(0)
            .tupleValue()
            .size() == 0
    );
  }
}
