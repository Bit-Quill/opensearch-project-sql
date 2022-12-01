/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The extension of ExprType in OpenSearch.
 */
@EqualsAndHashCode
public class OpenSearchDataType implements ExprType {

  public enum Type {
    Text("text"),
    Keyword("keyword"),
    Ip("ip"),
    GeoPoint("geo_point"),
    Binary("binary"),
    Date("date"),
    Nested("nested"),
    Byte("byte"),
    Short("short"),
    Integer("integer"),
    Long("long"),
    Float("float"),
    HalfFloat("half_float"),
    ScaledFloat("scaled_float"),
    Double("double"),
    Boolean("boolean");
    // TODO: nested, object, ranges, geo shape, point, shape, scaled_float

    private String name;

    Type(String name) {
      this.name = name;
    }
  }

  //@Getter
  @EqualsAndHashCode.Exclude
  private Type type;

  // resolved ExprCoreType
  @Getter
  private ExprCoreType exprCoreType;

  public OpenSearchDataType(Type type) {
    this.type = type;
    switch (type) {
      // TODO update these 2 below #1038 https://github.com/opensearch-project/sql/issues/1038
      case Text:
      case Keyword: exprCoreType = ExprCoreType.STRING; break;
      case Byte: exprCoreType = ExprCoreType.BYTE; break;
      case Short: exprCoreType = ExprCoreType.SHORT; break;
      case Integer: exprCoreType = ExprCoreType.INTEGER; break;
      case Long: exprCoreType = ExprCoreType.LONG; break;
      case HalfFloat:
      case Float: exprCoreType = ExprCoreType.FLOAT; break;
      case ScaledFloat:
      case Double: exprCoreType = ExprCoreType.DOUBLE; break;
      case Boolean: exprCoreType = ExprCoreType.BOOLEAN; break;
      // TODO: check formats, it could allow TIME or DATE only
      case Date: exprCoreType = ExprCoreType.TIMESTAMP; break;
      // TODO validate that it works ok, prev impl had `nested` -> ARRAY, `object` -> STRUCT
      case Nested: exprCoreType = ExprCoreType.STRUCT; break;
      // TODO
      case GeoPoint:
      case Binary:
      case Ip:
      default:
        exprCoreType = ExprCoreType.UNKNOWN;
    }
  }

  public OpenSearchDataType(ExprCoreType type) {
    this.exprCoreType = type;
    // TODO set type?
  }

  // nested has properties, text could have fields
  // TODO could be fields in other types?
  // TODO what is better structure - map (nested maps) or a tree?
  @Getter
  @EqualsAndHashCode.Exclude
  Map<String, OpenSearchDataType> fieldsOrProperties = new HashMap<>();

  public Boolean hasNestedFields() {
    return fieldsOrProperties.size() > 0;
  }

  /**
   * Date formats stored in index mapping. Applicable for {@link Type.Date} only.
   */
  @Getter
  @EqualsAndHashCode.Exclude
  private List<String> formats;

  public void setFormats(String formats) {
    if (formats == null || formats.isEmpty()) {
      this.formats = List.of();
    } else {
      this.formats = Arrays.stream(formats.split("\\|\\|"))
          .map(String::trim).collect(Collectors.toList());
    }
  }

  @Override
  public String typeName() {
    return type.toString();
    /*
    switch (type) {
      case GeoPoint: return "geo_point";
      case HalfFloat: return "half_point";
      case ScaledFloat: return "scaled_point";
      // TODO update these 2 below #1038 https://github.com/opensearch-project/sql/issues/1038
      case Text: return "string";
      case Keyword: return "string";
      case Ip: return "ip";
      case Binary: return "binary";
      case Nested: return "nested";
    }
    throw new IllegalArgumentException(type.toString());
    */
  }

  @Override
  public String legacyTypeName() {
    throw new UnsupportedOperationException();
  }

  private OpenSearchDataType cloneWithoutNested() {
    var copy = new OpenSearchDataType(type);
    copy.exprCoreType = exprCoreType;
    copy.formats = formats;
    return copy;
  }

  public static Map<String, OpenSearchDataType> traverseAndFlatten(
      Map<String, OpenSearchDataType> tree) {
    Map<String, OpenSearchDataType> result = new HashMap<>();
    for (var entry : tree.entrySet()) {
      result.put(entry.getKey(), entry.getValue().cloneWithoutNested());
      if (entry.getValue().hasNestedFields()) { // can be omitted
        result.putAll(
            traverseAndFlatten(entry.getValue().fieldsOrProperties)
                .entrySet().stream()
                .collect(Collectors.toMap(
                    e -> String.format("%s.%s", entry.getKey(), e.getKey()),
                    e -> e.getValue())));
      }
    }
    return result;
  }
}
