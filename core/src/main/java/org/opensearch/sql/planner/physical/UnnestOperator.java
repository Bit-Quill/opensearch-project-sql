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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.ReferenceExpression;

@EqualsAndHashCode(callSuper = false)
public class UnnestOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final Set<String> fields; // Needs to be a Set to match legacy implementation
  @Getter
  List<Map<String, ExprValue>> result = new ArrayList<>();
  @EqualsAndHashCode.Exclude
  private ListIterator<Map<String, ExprValue>> flattenedResult = result.listIterator();

  /**
   * Constructor for UnnestOperator with list of map as arg.
   * @param input : PhysicalPlan input.
   * @param fields : List of all fields and paths for nested fields.
   */
  public UnnestOperator(PhysicalPlan input, List<Map<String, ReferenceExpression>> fields) {
    this.input = input;
    this.fields = fields.stream()
        .map(m -> m.get("field").toString())
        .collect(Collectors.toSet());
  }

  /**
   * Constructor for UnnestOperator with Set of fields.
   * @param input : PhysicalPlan input.
   * @param fields : List of all fields for nested fields.
   */
  public UnnestOperator(PhysicalPlan input, Set<String> fields) {
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
  public void open() {
    super.open();
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
      for (String field : fields) {
        result = flatten(field, inputValue, result);
      }
      if (result.isEmpty()) {
        return new ExprTupleValue(new LinkedHashMap<>());
      }

//      addNonNestedFieldsToResult(inputValue, result);
      flattenedResult = result.listIterator();
    }
    return new ExprTupleValue(new LinkedHashMap<>(flattenedResult.next()));
  }

//  public void addNonNestedFieldsToResult(ExprValue row, List<Map<String, ExprValue>> result) {
//    // legacy does not currently support arrays. Later we may want to implement a cartesian product with
//    //  PartiQL for non-nested type arrays. For now we only care about single layer object types.
//
//    // TODO we want to support arrays so that it's easy to implement partiql. We need a reference to all select items and then we don't care which are of nested type.
//    //  this can help fix our issue with using PartiQL syntax,
//    //   question, are we generating the same DSL for push down? In the valueMap all fields are being returned, not just the ones used in the SELECT clause.
//    //    If we can get the list this far then doing the final product is automatic. Is this a problem with pushdown though? If the DSL is general enough that it
//    //     returns all fields in the row than perhaps we just treat the qualified name as an alias in the project operator and as a field in the UnnestOperator.
//    for (int i = 0; i < result.size(); i++) {
//      for (var inputMap : row.tupleValue().entrySet()) {
//        if (!result.get(i).containsKey(inputMap.getKey())) {
//          var blah = result.get(i);
//          result.get(i).put(inputMap.getKey(), inputMap.getValue());
//        }
//      }
//    }
//  }

  /**
   * Simplifies the structure of row's source Map by flattening it,
   * making the full path of an object the key
   * and the Object it refers to the value.
   *
   * <p>Sample input:
   * keys = ['comments.likes']
   * row = comments: {
   * likes: 2
   * }
   *
   * <p>Return:
   * flattenedRow = {comment.likes: 2}
   *
   * @param nestedField : Field to query in row
   * @param row : Row returned from OS
   * @param prevList : List of previous nested calls
   * @return : List of nested select items or cartesian product of nested calls
   */
  @SuppressWarnings("unchecked")
  private List<Map<String, ExprValue>> flatten(
      String nestedField, ExprValue row, List<Map<String, ExprValue>> prevList
  ) {
    List<Map<String, ExprValue>> copy = new ArrayList<>();
    List<Map<String, ExprValue>> newList = new ArrayList<>();

    ExprValue nestedObj = null;
    getNested(nestedField, nestedField, row, copy, nestedObj);

    // Only one field in select statement
    if (prevList.size() == 0) {
      return copy;
    }

    // Generate cartesian product
    for (Map<String, ExprValue> prevMap : prevList) {
      for (Map<String, ExprValue> newMap : copy) {
        newList.add(Stream.of(newMap, prevMap)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue)));
      }
    }
    return newList;
  }

  /**
   * Retrieve nested field(s) in row.
   *
   * @param field : path for nested field.
   * @param nestedField : current level to nested field path.
   * @param row : Row to resolve nested field.
   * @param ret : List to add nested field to.
   * @param nestedObj : object at current nested level.
   * @return : Object at current nested level.
   */
  private void getNested(
      String field, String nestedField, ExprValue row,
      List<Map<String, ExprValue>> ret, ExprValue nestedObj
  ) {
    ExprValue currentObj = (nestedObj == null) ? row : nestedObj;
    String[] splitKeys = nestedField.split("\\.");

    if (currentObj instanceof ExprTupleValue) {
      ExprTupleValue currentMap = (ExprTupleValue) currentObj;
      if (currentMap.tupleValue().containsKey(splitKeys[0])) {
        currentObj = currentMap.tupleValue().get(splitKeys[0]);
      } else {
        currentObj = null;
      }
    } else if (currentObj instanceof ExprCollectionValue)  {
      ExprValue arrayObj = currentObj;
      for (int x = 0; x < arrayObj.collectionValue().size(); x++) {
        currentObj = arrayObj.collectionValue().get(x);
        getNested(field, nestedField, row, ret, currentObj);
        currentObj = null;
      }
    } else { // Final recursion did not match nested field
      currentObj = null;
    }

    // Return final nested result
    if (StringUtils.substringAfterLast(field, ".").equals(nestedField)
        && currentObj != null) {
      ret.add(Map.of(field, currentObj));
    } else if (currentObj != null) {
      getNested(field, nestedField.substring(nestedField.indexOf(".") + 1), row, ret, currentObj);
    }
  }
}
