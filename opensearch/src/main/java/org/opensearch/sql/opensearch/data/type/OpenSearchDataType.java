/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * The extension of ExprType in OpenSearch.
 */
@EqualsAndHashCode
public class OpenSearchDataType implements ExprType, Serializable {

  public enum Type {
    Text("text"),
    Keyword("keyword"),
    Ip("ip"),
    GeoPoint("geo_point"),
    Binary("binary"),
    Date("date"),
    Object("object"),
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
    // TODO: ranges, geo shape, point, shape

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String toString() {
      return name;
    }
  }

  @EqualsAndHashCode.Exclude
  protected Type type;

  // resolved ExprCoreType
  protected ExprCoreType exprCoreType;

  // Need to avoid returning `UNKNOWN` for `OpenSearch*Type`s, e.g. for IP
  public ExprType getExprType() {
    if (exprCoreType != ExprCoreType.UNKNOWN) {
      return exprCoreType;
    }
    return this;
  }

  public static OpenSearchDataType of(Type type) {
    ExprCoreType exprCoreType = ExprCoreType.UNKNOWN;
    switch (type) {
      // TODO update these 2 below #1038 https://github.com/opensearch-project/sql/issues/1038
      case Text: return new OpenSearchTextType();
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
      case Object: exprCoreType = ExprCoreType.STRUCT; break;
      case Nested: exprCoreType = ExprCoreType.ARRAY; break;
      case GeoPoint: return new OpenSearchGeoPointType();
      case Binary: return new OpenSearchBinaryType();
      case Ip: return new OpenSearchIpType();
      default:
        throw new IllegalArgumentException(type.toString());
    }
    var res = new OpenSearchDataType(type);
    res.exprCoreType = exprCoreType;
    return res;
  }

  protected OpenSearchDataType(Type type) {
    this.type = type;
  }

  public static OpenSearchDataType of(ExprType type) {
    if (type instanceof OpenSearchDataType) {
      return (OpenSearchDataType) type;
    }
    return new OpenSearchDataType((ExprCoreType) type);
  }

  protected OpenSearchDataType(ExprCoreType type) {
    this.exprCoreType = type;
  }

  protected OpenSearchDataType() { }

  // object has properties
  @Getter
  @EqualsAndHashCode.Exclude
  Map<String, OpenSearchDataType> properties = new HashMap<>();

  // text could have fields
  @Getter
  @EqualsAndHashCode.Exclude
  Map<String, OpenSearchDataType> fields = new HashMap<>();

  @Override
  public String typeName() {
    if (type == null) {
      return exprCoreType.typeName();
    }
    return type.toString().toLowerCase();
  }

  @Override
  public String legacyTypeName() {
    return typeName();
  }

  /**
   * Clone type object without {@link #properties} - without info nested about nested object types.
   * @return A clone.
   */
  protected OpenSearchDataType cloneEmpty() {
    var copy = type != null ? of(type) : new OpenSearchDataType(exprCoreType);
    copy.fields = fields; //TODO do we need to clone object?
    copy.exprCoreType = exprCoreType;
    return copy;
  }

  public static Map<String, OpenSearchDataType> traverseAndFlatten(
      Map<String, OpenSearchDataType> tree) {
    Map<String, OpenSearchDataType> result = new HashMap<>();
    for (var entry : tree.entrySet()) {
      result.put(entry.getKey(), entry.getValue().cloneEmpty());
      result.putAll(
          traverseAndFlatten(entry.getValue().properties)
              .entrySet().stream()
              .collect(Collectors.toMap(
                  e -> String.format("%s.%s", entry.getKey(), e.getKey()),
                  e -> e.getValue())));
    }
    return result;
  }
}
