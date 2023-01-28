/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * The extension of ExprType in OpenSearch.
 */
@EqualsAndHashCode
public class OpenSearchDataType implements ExprType, Serializable {

  /**
   * The mapping (OpenSearch engine) type.
   */
  public enum MappingType {
    Invalid(null),
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

    MappingType(String name) {
      this.name = name;
    }

    public String toString() {
      return name;
    }
  }

  @EqualsAndHashCode.Exclude
  protected MappingType mappingType;

  // resolved ExprCoreType
  protected ExprCoreType exprCoreType;

  /**
   * Get a simplified type {@link ExprCoreType} if possible.
   * To avoid returning `UNKNOWN` for `OpenSearch*Type`s, e.g. for IP, returns itself.
   * @return An {@link ExprType}.
   */
  public ExprType getExprType() {
    if (exprCoreType != ExprCoreType.UNKNOWN) {
      return exprCoreType;
    }
    return this;
  }

  /**
   * Simple instances of OpenSearchDataType are created once during entire SQL engine lifetime
   * and cached there. This reduces memory usage and increases type comparison.
   * Note: Types with non-empty fields and properties are not cached.
   */
  private static final Map<String, OpenSearchDataType> instances = new HashMap<>();

  /**
   * A constructor function which builds proper `OpenSearchDataType` for given mapping `Type`.
   * @param mappingType A mapping type.
   * @return An instance or inheritor of `OpenSearchDataType`.
   */
  public static OpenSearchDataType of(MappingType mappingType) {
    if (instances.containsKey(mappingType.toString())) {
      return instances.get(mappingType.toString());
    }
    ExprCoreType exprCoreType = ExprCoreType.UNKNOWN;
    switch (mappingType) {
      // TODO update these 2 below #1038 https://github.com/opensearch-project/sql/issues/1038
      case Text: return OpenSearchTextType.getInstance();
      case Keyword: exprCoreType = ExprCoreType.STRING;
        break;
      case Byte: exprCoreType = ExprCoreType.BYTE;
        break;
      case Short: exprCoreType = ExprCoreType.SHORT;
        break;
      case Integer: exprCoreType = ExprCoreType.INTEGER;
        break;
      case Long: exprCoreType = ExprCoreType.LONG;
        break;
      case HalfFloat: exprCoreType = ExprCoreType.FLOAT;
        break;
      case Float: exprCoreType = ExprCoreType.FLOAT;
        break;
      case ScaledFloat: exprCoreType = ExprCoreType.DOUBLE;
        break;
      case Double: exprCoreType = ExprCoreType.DOUBLE;
        break;
      case Boolean: exprCoreType = ExprCoreType.BOOLEAN;
        break;
      // TODO: check formats, it could allow TIME or DATE only
      case Date: exprCoreType = ExprCoreType.TIMESTAMP;
        break;
      case Object: exprCoreType = ExprCoreType.STRUCT;
        break;
      case Nested: exprCoreType = ExprCoreType.ARRAY;
        break;
      case GeoPoint: return OpenSearchGeoPointType.getInstance();
      case Binary: return OpenSearchBinaryType.getInstance();
      case Ip: return OpenSearchIpType.getInstance();
      default:
        throw new IllegalArgumentException(mappingType.toString());
    }
    var res = new OpenSearchDataType(mappingType);
    res.exprCoreType = exprCoreType;
    instances.put(mappingType.toString(), res);
    return res;
  }

  /**
   * A constructor function which builds proper `OpenSearchDataType` for given mapping `Type`.
   * Designed to be called by the mapping parser only (and tests).
   * @param mappingType A mapping type.
   * @param properties Properties to set.
   * @param fields Fields to set.
   * @return An instance or inheritor of `OpenSearchDataType`.
   */
  public static OpenSearchDataType of(MappingType mappingType,
                                      Map<String, OpenSearchDataType> properties,
                                      Map<String, OpenSearchDataType> fields) {
    var res = of(mappingType);
    if (!properties.isEmpty() || !fields.isEmpty()) {
      // Clone to avoid changing the singleton instance.
      res = res.cloneEmpty();
      res.properties = ImmutableMap.copyOf(properties);
      res.fields = ImmutableMap.copyOf(fields);
    }
    return res;
  }

  protected OpenSearchDataType(MappingType mappingType) {
    this.mappingType = mappingType;
  }

  /**
   * A constructor function which builds proper `OpenSearchDataType` for given {@link ExprType}.
   * @param type A type.
   * @return An instance of `OpenSearchDataType`.
   */
  public static OpenSearchDataType of(ExprType type) {
    if (type instanceof OpenSearchDataType) {
      return (OpenSearchDataType) type;
    }
    if (instances.containsKey(type.toString())) {
      return instances.get(type.toString());
    }
    var res = new OpenSearchDataType((ExprCoreType) type);
    instances.put(type.toString(), res);
    return res;
  }

  protected OpenSearchDataType(ExprCoreType type) {
    this.exprCoreType = type;
  }

  protected OpenSearchDataType() {
  }

  // For datatypes with properties (example: object and nested types)
  // a read-only collection
  @Getter
  @EqualsAndHashCode.Exclude
  Map<String, OpenSearchDataType> properties = ImmutableMap.of();

  // text could have fields
  // a read-only collection
  @EqualsAndHashCode.Exclude
  Map<String, OpenSearchDataType> fields = ImmutableMap.of();

  @Override
  // Called when building TypeEnvironment and when serializing PPL response
  public String typeName() {
    // To avoid breaking changes return `string` for `typeName` call (PPL) and `text` for
    // `legacyTypeName` call (SQL). See more: https://github.com/opensearch-project/sql/issues/1296
    if (legacyTypeName().equals("text")) {
      return "string";
    }
    return legacyTypeName();
  }

  @Override
  // Called when serializing SQL response
  public String legacyTypeName() {
    if (mappingType == null) {
      return exprCoreType.typeName();
    }
    return mappingType.toString().toLowerCase();
  }

  /**
   * Clone type object without {@link #properties} - without info about nested object types.
   * @return A cloned object.
   */
  @SneakyThrows
  protected OpenSearchDataType cloneEmpty() {
    // This trick is required to ensure that the clone has the same type as clonee.
    // Otherwise, clone of OpenSearchTextType becomes OpenSearchDataType.
    // An alternate option is to @Override this function in all inheritors.
    // Requires all derived types to have a default constructor.
    var ctor = this.getClass().getDeclaredConstructor();
    ctor.setAccessible(true);
    var copy = (OpenSearchDataType)ctor.newInstance();
    copy.mappingType = mappingType;
    copy.fields = fields;
    copy.exprCoreType = exprCoreType;
    return copy;
  }

  /**
   * Flattens mapping tree into a single layer list of objects (pairs of name-types actually),
   * which don't have nested types.
   * See {@link OpenSearchDataTypeTest#traverseAndFlatten() test} for example.
   * @param tree A list of `OpenSearchDataType`s - map between field name and its type.
   * @return A list of all `OpenSearchDataType`s from given map on the same nesting level (1).
   *         Nested object names are prefixed by names of their host.
   */
  public static Map<String, OpenSearchDataType> traverseAndFlatten(
      Map<String, OpenSearchDataType> tree) {
    final Map<String, OpenSearchDataType> result = new LinkedHashMap<>();
    BiConsumer<Map<String, OpenSearchDataType>, String> visitLevel = new BiConsumer<>() {
      @Override
      public void accept(Map<String, OpenSearchDataType> subtree, String prefix) {
        for (var entry : subtree.entrySet()) {
          String entryKey = entry.getKey();
          var nextPrefix = prefix.isEmpty() ? entryKey : String.format("%s.%s", prefix, entryKey);
          result.put(nextPrefix, entry.getValue().cloneEmpty());
          var nextSubtree = entry.getValue().getProperties();
          if (!nextSubtree.isEmpty()) {
            accept(nextSubtree, nextPrefix);
          }
        }
      }
    };
    visitLevel.accept(tree, "");
    return result;
  }

  /**
   * Resolve type of identified from parsed mapping tree.
   * @param tree Parsed mapping tree (not flattened).
   * @param id An identifier.
   * @return Resolved OpenSearchDataType or null if not found.
   */
  public static OpenSearchDataType resolve(Map<String, OpenSearchDataType> tree, String id) {
    for (var item : tree.entrySet()) {
      if (item.getKey().equals(id)) {
        return item.getValue();
      }
      OpenSearchDataType result = resolve(item.getValue().getProperties(), id);
      if (result != null) {
        return result;
      }
    }
    return null;
  }
}
