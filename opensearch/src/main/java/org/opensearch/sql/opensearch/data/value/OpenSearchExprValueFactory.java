/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.utils.Content;
import org.opensearch.sql.opensearch.data.utils.ObjectContent;
import org.opensearch.sql.opensearch.data.utils.OpenSearchJsonContent;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

/**
 * Construct ExprValue from OpenSearch response.
 */
public class OpenSearchExprValueFactory {
  /**
   * The Mapping of Field and ExprType.
   */
  @Setter
  private Map<String, OpenSearchDataType> typeMapping;

  @Getter
  @Setter
  private OpenSearchAggregationResponseParser parser;

  private static final String TOP_PATH = "";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<ExprType, BiFunction<Content, OpenSearchDataType, ExprValue>> typeActionMap =
      new ImmutableMap.Builder<ExprType, BiFunction<Content, OpenSearchDataType, ExprValue>>()
          .put(new OpenSearchDataType(OpenSearchDataType.Type.Integer),
              (c, dt) -> new ExprIntegerValue(c.intValue()))
          .put(new OpenSearchDataType(OpenSearchDataType.Type.Long),
              (c, dt) -> new ExprLongValue(c.longValue()))
          .put(new OpenSearchDataType(OpenSearchDataType.Type.Short),
              (c, dt) -> new ExprShortValue(c.shortValue()))
          .put(new OpenSearchDataType(OpenSearchDataType.Type.Byte),
              (c, dt) -> new ExprByteValue(c.byteValue()))
          .put(new OpenSearchDataType(OpenSearchDataType.Type.Float),
              (c, dt) -> new ExprFloatValue(c.floatValue()))
          .put(new OpenSearchDataType(OpenSearchDataType.Type.Double),
              (c, dt) -> new ExprDoubleValue(c.doubleValue()))
          // TODO commented out because `Text` and `Keyword` mapped to the same ExprCoreType
          //.put(new OpenSearchDataType(OpenSearchDataType.Type.Text),
          //    (c, dt) -> new OpenSearchExprTextValue(c.stringValue()))
          .put(new OpenSearchDataType(OpenSearchDataType.Type.Keyword),
              (c, dt) -> new ExprStringValue(c.stringValue()))
          // TODO do we need entry for `new OpenSearchDataType(STRING)` ?
          .put(new OpenSearchDataType(OpenSearchDataType.Type.Boolean),
              (c, dt) -> ExprBooleanValue.of(c.booleanValue()))
          .put(new OpenSearchDataType(TIMESTAMP), this::constructTimestamp)
          .put(new OpenSearchDataType(DATE),
              (c, dt) -> new ExprDateValue(constructTimestamp(c, dt).dateValue().toString()))
          .put(new OpenSearchDataType(TIME),
              (c, dt) -> new ExprTimeValue(constructTimestamp(c, dt).timeValue().toString()))
          .put(new OpenSearchDataType(DATETIME),
              (c, dt) -> new ExprDatetimeValue(constructTimestamp(c, dt).datetimeValue()))
          // TODO commented out because these types mapped to the same ExprCoreType : UNKNOWN
          /*
          .put(new OpenSearchDataType(Ip),
              (c, dt) -> new OpenSearchExprIpValue(c.stringValue()))
          .put(new OpenSearchDataType(GeoPoint),
              (c, dt) -> new OpenSearchExprGeoPointValue(c.geoValue().getLeft(),
                  c.geoValue().getRight()))
          .put(new OpenSearchDataType(Binary),
              (c, dt) -> new OpenSearchExprBinaryValue(c.stringValue()))
           */
          .build();

  /**
   * Constructor of OpenSearchExprValueFactory.
   */
  public OpenSearchExprValueFactory(Map<String, OpenSearchDataType> typeMapping) {
    this.typeMapping = typeMapping;
  }

  /**
   * The struct construction has the following assumption. 1. The field has OpenSearch Object
   * data type. https://www.elastic.co/guide/en/elasticsearch/reference/current/object.html 2. The
   * deeper field is flattened in the typeMapping. e.g. {"employ", "STRUCT"} {"employ.id",
   * "INTEGER"} {"employ.state", "STRING"}
   */
  public ExprValue construct(String jsonString) {
    try {
      return parse(new OpenSearchJsonContent(OBJECT_MAPPER.readTree(jsonString)), TOP_PATH,
          Optional.of(STRUCT));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(String.format("invalid json: %s.", jsonString), e);
    }
  }

  /**
   * Construct ExprValue from field and its value object. Throw exception if trying
   * to construct from field of unsupported type.
   * Todo, add IP, GeoPoint support after we have function implementation around it.
   *
   * @param field field name
   * @param value value object
   * @return ExprValue
   */
  public ExprValue construct(String field, Object value) {
    return parse(new ObjectContent(value), field, type(field));
  }

  private ExprValue parse(Content content, String field, Optional<ExprType> fieldType) {
    if (content.isNull() || !fieldType.isPresent()) {
      return ExprNullValue.of();
    }

    ExprType type = fieldType.get();
    if (type == STRUCT) {
      return parseStruct(content, field);
    } else if (type == ARRAY) {
      return parseArray(content, field);
    } else {
      if (typeActionMap.containsKey(type)) {
        return typeActionMap.get(type).apply(content, (OpenSearchDataType)type);
      } else {
        throw new IllegalStateException(
            String.format(
                "Unsupported type: %s for value: %s.", type.typeName(), content.objectValue()));
      }
    }
  }

  /**
   * In OpenSearch, it is possible field doesn't have type definition in mapping.
   * but has empty value. For example, {"empty_field": []}.
   */
  private Optional<ExprType> type(String field) {
    return Optional.ofNullable(typeMapping.get(field));
  }

  /**
   * Only default strict_date_optional_time||epoch_millis is supported,
   * strict_date_optional_time_nanos||epoch_millis if field is date_nanos.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/date_nanos.html
   * The customized date_format is not supported.
   */
  private ExprValue constructTimestamp(Content value, OpenSearchDataType __) {
    try {
      return new ExprTimestampValue(
          // Using OpenSearch DateFormatters for now.
          DateFormatters.from(DATE_TIME_FORMATTER.parse(value.stringValue())).toInstant());
    } catch (DateTimeParseException e) {
      throw new IllegalStateException(
          String.format(
              "Construct ExprTimestampValue from \"%s\" failed, unsupported date format.", value),
          e);
    }
  }

  private ExprValue parseStruct(Content content, String prefix) {
    LinkedHashMap<String, ExprValue> result = new LinkedHashMap<>();
    content.map().forEachRemaining(entry -> result.put(entry.getKey(),
        parse(entry.getValue(),
            makeField(prefix, entry.getKey()),
            type(makeField(prefix, entry.getKey())))));
    return new ExprTupleValue(result);
  }

  /**
   * Todo. ARRAY is not support now. In Elasticsearch, there is no dedicated array data type.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html. The similar data
   * type is nested, but it can only allow a list of objects.
   */
  private ExprValue parseArray(Content content, String prefix) {
    List<ExprValue> result = new ArrayList<>();
    content.array().forEachRemaining(v -> {
      // ExprCoreType.ARRAY does not indicate inner elements type. OpenSearch nested will be an
      // array of structs, otherwise parseArray currently only supports array of strings.
      if (v.isString()) {
        result.add(parse(v, prefix, Optional.of(STRING)));
      } else {
        result.add(parse(v, prefix, Optional.of(STRUCT)));
      }
    });
    return new ExprCollectionValue(result);
  }

  private String makeField(String path, String field) {
    return path.equalsIgnoreCase(TOP_PATH) ? field : String.join(".", path, field);
  }
}
