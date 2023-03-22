/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
  List<String> nonNestedFields = new ArrayList<>();
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
  public boolean hasNext() {
    return input.hasNext() || flattenedResult.hasNext();
  }


  @Override
  public ExprValue next() {
    if (!flattenedResult.hasNext()) {
      result.clear();
      nonNestedFields.clear();

      ExprValue inputValue = input.next();
      generateNonNestedFieldsMap(inputValue);
      for (String field : fields) {
        result = flatten(field, inputValue, result, true);
      }

      for (String nonNestedField : nonNestedFields) {
        result = flatten(nonNestedField, inputValue, result, false);
      }

      if (result.isEmpty()) {
        return new ExprTupleValue(new LinkedHashMap<>());
      }

      flattenedResult = result.listIterator();
    }
    return new ExprTupleValue(new LinkedHashMap<>(flattenedResult.next()));
  }

  /**
   * Generate list of non-nested fields that are in inputMap, but not in the member variable
   * fields list.
   * @param inputMap : Row to parse non-nested fields.
   */
  public void generateNonNestedFieldsMap(ExprValue inputMap) {

    for (Map.Entry<String, ExprValue> inputField : inputMap.tupleValue().entrySet()) {
      boolean foundNestedField = false;
      for (String field : this.fields) {
        if ((field).split("\\.")[0].equalsIgnoreCase(inputField.getKey())) {
          foundNestedField = true;
          break;
        }
      }

      if (!foundNestedField) {
        boolean nestingComplete = false;
        String nonNestedField = inputField.getKey();
        ExprValue currentObj = inputField.getValue();
        while (!nestingComplete) {
          if (currentObj instanceof ExprTupleValue) {
            for (var it : currentObj.tupleValue().entrySet()) {
              currentObj = it.getValue();
              nonNestedField += "." + it.getKey();
              // We only care about the keys
              break;
            }
          } else if (currentObj instanceof ExprCollectionValue) {
            currentObj = currentObj.collectionValue().get(0);
          } else {
            nestingComplete = true;
          }
        }
        this.nonNestedFields.add(nonNestedField);
      }
    }
  }


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
   * @param nestedField : Field to query in row.
   * @param row : Row returned from OS.
   * @param prevList : List of previous nested calls.
   * @param supportArrays : When false we do not need to execute a cross join.
   * @return : List of nested select items or cartesian product of nested calls.
   */
  @SuppressWarnings("unchecked")
  private List<Map<String, ExprValue>> flatten(
      String nestedField, ExprValue row, List<Map<String, ExprValue>> prevList, boolean supportArrays
  ) {
    List<Map<String, ExprValue>> copy = new ArrayList<>();
    List<Map<String, ExprValue>> newList = new ArrayList<>();

    ExprValue nestedObj = null;
    getNested(nestedField, nestedField, row, copy, nestedObj, supportArrays);

    // Only one field in select statement
    if (prevList.size() == 0) {
      return copy;
    }

    if (supportArrays) {
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
    } else {
      for (Map<String, ExprValue> resultsMap : this.result) {
        for (Map<String, ExprValue> nonNestedMap : copy) {
          resultsMap.putAll(nonNestedMap);
        }
      }
      return this.result;
    }
  }

  /**
   * Retrieve nested field(s) in row.
   *
   * @param field : Path for nested field.
   * @param nestedField : Current level to nested field path.
   * @param row : Row to resolve nested field.
   * @param ret : List to add nested field to.
   * @param nestedObj : Object at current nested level.
   * @param supportArrays : Only first index of arrays is supports when false.
   * @return : Object at current nested level.
   */
  private void getNested(
      String field, String nestedField, ExprValue row,
      List<Map<String, ExprValue>> ret, ExprValue nestedObj,
      boolean supportArrays
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
      if (supportArrays) {
        for (int x = 0; x < arrayObj.collectionValue().size(); x++) {
          currentObj = arrayObj.collectionValue().get(x);
          getNested(field, nestedField, row, ret, currentObj, supportArrays);
          currentObj = null;
        }
      } else { // TODO remove when arrays are supported.
        currentObj = arrayObj.collectionValue().get(0);
        getNested(field, nestedField, row, ret, currentObj, supportArrays);
        currentObj = null;
      }
    } else { // Final recursion did not match nested field
      if (field.equalsIgnoreCase(nestedField)) {
        currentObj = null;
      }
    }

    // Return final nested result
    if (currentObj != null
        && (StringUtils.substringAfterLast(field, ".").equals(nestedField) || !field.contains("."))) {
        ret.add(new HashMap<>(Map.of(field, currentObj)));
    } else if (currentObj != null) {
      getNested(field, nestedField.substring(nestedField.indexOf(".") + 1),
          row, ret, currentObj, supportArrays);
    }
  }
}
