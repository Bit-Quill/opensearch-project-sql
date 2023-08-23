/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.testset;

import static java.util.stream.Collectors.joining;
import static org.opensearch.sql.legacy.utils.StringUtils.unquoteSingleField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.json.JSONObject;
import org.opensearch.sql.legacy.utils.StringUtils;

/** Test data set */
public class TestDataSet {

  private final String tableName;
  private final String schema;
  private final List<Object[]> dataRows;

  public TestDataSet(String tableName, String schemaFileContent, String dataFileContent) {
    this.tableName = tableName;
    this.schema = schemaFileContent;
    this.dataRows = convertStringDataToActualType(splitColumns(dataFileContent, ','));
  }

  public String getTableName() {
    return tableName;
  }

  public String getSchema() {
    return schema;
  }

  public List<Object[]> getDataRows() {
    return dataRows;
  }

  /** Split columns in each line by separator and ignore escaped separator(s) in quoted string. */
  private List<String[]> splitColumns(String content, char separator) {
    List<String[]> result = new ArrayList<>();
    for (String line : content.split("\\r?\\n")) {

      List<String> columns = new ArrayList<>();
      boolean isQuoted = false;
      int start = 0;
      for (int i = 0; i < line.length(); i++) {

        char c = line.charAt(i);
        if (c == separator) {
          if (isQuoted) { // Ignore comma(s) in quoted string
            continue;
          }

          String column = line.substring(start, i);
          columns.add(unquoteSingleField(column, "\""));
          start = i + 1;

        } else if (c == '\"') {
          isQuoted = !isQuoted;
        }
      }

      columns.add(unquoteSingleField(line.substring(start), "\""));
      result.add(columns.toArray(new String[0]));
    }
    return result;
  }

  /**
   * Convert column string values (read from CSV file) to objects of its real type based on the type
   * information in index mapping file.
   */
  private List<Object[]> convertStringDataToActualType(List<String[]> rows) {
    JSONObject types = new JSONObject(schema);
    String[] columnNames = rows.get(0);

    List<Object[]> result = new ArrayList<>();
    result.add(columnNames);

    rows.stream()
        .skip(1)
        .map(row -> convertStringArrayToObjectArray(types, columnNames, row))
        .forEach(result::add);
    return result;
  }

  private Object[] convertStringArrayToObjectArray(
      JSONObject types, String[] columnNames, String[] row) {
    Object[] result = new Object[row.length];
    for (int i = 0; i < row.length; i++) {
      String colName = columnNames[i];
      String colTypePath = "/mappings/properties/" + colName;
      String colType = ((JSONObject) types.query(colTypePath)).getString("type");
      result[i] = convertStringToObject(colType, row[i]);
    }
    return result;
  }

  private Object convertStringToObject(String type, String str) {
    if (str.isEmpty()) {
      return null;
    }

    switch (type.toLowerCase()) {
      case "text":
      case "keyword":
      case "date":
      case "time":
      case "timestamp":
        return str;
      case "integer":
        return Integer.valueOf(str);
      case "float":
      case "half_float":
        return Float.valueOf(str);
      case "double":
        return Double.valueOf(str);
      case "boolean":
        return Boolean.valueOf(str);
      default:
        throw new IllegalStateException(
            StringUtils.format("Data type %s is not supported yet for value: %s", type, str));
    }
  }

  @Override
  public String toString() {
    int total = dataRows.size();
    return String.format(
            "Test data set:\n Table name: %s\n Schema: %s\n Data rows (first 5 in %d):",
            tableName, schema, total)
        + dataRows.stream().limit(5).map(Arrays::toString).collect(joining("\n ", "\n ", "\n"));
  }
}
