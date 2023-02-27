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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.ReferenceExpression;

@ToString
@EqualsAndHashCode(callSuper = false)
public class UnnestOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final Map<String, ReferenceExpression> fields; // Needs to be a HashMap of (path, field) to match legacy implementation
  @Getter
  List<Map<String, ExprValue>> result = new ArrayList<>();
  private ListIterator<Map<String, ExprValue>> flattenedResult = result.listIterator();

  public UnnestOperator(PhysicalPlan input, List<Map<String, ReferenceExpression>> fields) {
    this.input = input;
    this.fields = Stream.of(fields)
        .flatMap(list -> list.stream())
        .collect(Collectors.toMap(m -> m.get("path").toString()
            ,m -> m.get("field")));
  }

  public UnnestOperator(PhysicalPlan input, Map<String, ReferenceExpression> fields) {
    this.input = input;
    this.fields = fields;
  }

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

      for (var field : fields.keySet()) {
        result = flatten(fields.get(field).toString(), inputValue, result);
      }

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
   *
   * @param row
   * @param ???
   * @param ???
   */
  @SuppressWarnings("unchecked")
  private List<Map<String, ExprValue>> flatten(String nestedField, ExprValue row, List<Map<String, ExprValue>> current) {
    String[] splitKeys = nestedField.split("\\.");
    List<Map<String, ExprValue>> copy = new ArrayList<>();
    List<Map<String, ExprValue>> brand_new = new ArrayList<>();

    ExprValue nestedObj = null;
    for (String splitKey : splitKeys) {
      nestedObj = flattenObject(nestedField, splitKey, row, copy, nestedObj);
      if (nestedObj == null) {
        break;
      }
    }

    if (current.size() == 0) {
      return copy;
    }

    for (Map<String, ExprValue> blah : current) {
      for (Map<String, ExprValue> hoo : copy) {
        Map<String, ExprValue> map3 = Stream.of(hoo, blah)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue));
        brand_new.add(map3);
      }
    }
    return brand_new;
  }

  private ExprValue flattenObject(String field, String nestedField, ExprValue row, List<Map<String, ExprValue>> ret, ExprValue nestedObj) {
    ExprValue currentObj = (nestedObj == null) ? row : nestedObj;

    if (currentObj instanceof ExprTupleValue) {
      ExprTupleValue currentMap = (ExprTupleValue) currentObj;
      if (!currentMap.tupleValue().containsKey(nestedField)) {
        return null;
      }
      currentObj = currentMap.tupleValue().get(nestedField);
    } else if (currentObj instanceof Map) {
      Map<String, ExprValue> currentMap = (Map<String, ExprValue>)currentObj;
      if (!currentMap.containsKey(nestedField)) {
        return null;
      }
      currentObj = currentMap.get(nestedField);
    } else if (currentObj instanceof ExprCollectionValue) {
      ExprValue arrayObj = currentObj;
      for (int x = 0; x < arrayObj.collectionValue().size() ; x++) {
        currentObj = arrayObj.collectionValue().get(x);
        flattenObject(field, nestedField.substring(nestedField.indexOf(".") + 1), row, ret, currentObj);
      }
      return null;
    }

    if (StringUtils.substringAfterLast(field, ".").equals(nestedField)) {
      ret.add(Map.of(field, currentObj));
      currentObj = null;
    }
    return currentObj;
  }
}
