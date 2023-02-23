/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.ReferenceExpression;

@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class UnnestOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final ReferenceExpression field;
  @Getter
  private final ReferenceExpression path; // TODO do we need this?
  List<Map<String, ExprValue>> result = new ArrayList<>();
  private ListIterator<Map<String, ExprValue>> flattenedResult = result.listIterator();

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitUnnest(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return input.hasNext() || flattenedResult.hasNext();
  }


  @Override
  public ExprValue next() {
    if (!flattenedResult.hasNext()) {
      result.clear();
      ExprValue inputValue = input.next();
      flatten(inputValue, result, this.field.toString());
      flattenedResult = result.listIterator();
    }
    return new ExprTupleValue(new LinkedHashMap<>(flattenedResult.next()));
  }


  /**
   * Simplifies the structure of row's source Map by flattening it, making the full path of an object the key
   * and the Object it refers to the value. This handles the case of regular object since nested objects will not
   * be in hit.source but rather in hit.innerHits
   * <p>
   * Sample input:
   * keys = ['comments.likes']
   * row = comments: {
   * likes: 2
   * }
   * <p>
   * Return:
   * flattenedRow = {comment.likes: 2}
   */


  /**
   * Simplifies the structure of row's source Map by flattening it, making the full path of an object the key
   * and the Object it refers to the value. This handles the case of regular object since nested objects will not
   * be in hit.source but rather in hit.innerHits
   * <p>
   * Sample input:
   * keys = ['comments.likes']
   * row = comments: {
   * likes: 2
   * }
   * <p>
   * Return:
   * flattenedRow = {comment.likes: 2}
   *
   * @param row
   * @param ret
   * @param keys
   */
  @SuppressWarnings("unchecked")
  private void flatten(ExprValue row, List<Map<String, ExprValue>> ret, String keys) {
    String[] splitKeys = keys.split("\\.");
    boolean found = true;
    Object currentObj = row;

    for (String splitKey : splitKeys) {
      if (currentObj instanceof ExprTupleValue) {
          ExprTupleValue currentMap = (ExprTupleValue) currentObj;
          if (!currentMap.tupleValue().containsKey(splitKey)) {
            found = false;
            break;
          }
        currentObj = currentMap.tupleValue().get(splitKey);
      } else if (currentObj instanceof Map) {
        Map<String, ExprValue> currentMap = (Map<String, ExprValue>)currentObj;
        if (!currentMap.containsKey(splitKey)) {
          found = false;
          break;
        }
        currentObj = currentMap.get(splitKey);
      } else if (currentObj instanceof ExprCollectionValue) {
        for (int i = 0; i < ((ExprCollectionValue) currentObj).collectionValue().size() ; i++) {
          ExprValue currentVal = ((ExprCollectionValue) currentObj).collectionValue().get(i);
          flatten(currentVal, ret, keys.substring(keys.indexOf(".") + 1));
        } // TODO handle primitive types in arrays?
        found = false;
      } else {
        // TODO what to do for primitive types
        found = false;
      }
    }

    if (found) {
      ret.add(Map.of(this.field.toString(), (ExprValue) currentObj));
    }
  }
}
