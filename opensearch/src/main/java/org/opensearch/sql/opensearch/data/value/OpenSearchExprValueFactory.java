/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.common.time.DateFormatter;
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
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
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
  private final Map<String, OpenSearchDataType> typeMapping;

  /**
   * Extend existing mapping by new data without overwrite.
   * Called from aggregation only {@link AggregationQueryBuilder#buildTypeMapping}.
   * @param typeMapping A data type mapping produced by aggregation.
   */
  public void extendTypeMapping(Map<String, OpenSearchDataType> typeMapping) {
    for (var field : typeMapping.keySet()) {
      // Prevent overwriting, because aggregation engine may be not aware
      // of all niceties of all types.
      if (!this.typeMapping.containsKey(field)) {
        this.typeMapping.put(field, typeMapping.get(field));
      }
    }
  }

  @Getter
  @Setter
  private OpenSearchAggregationResponseParser parser;

  private static final String TOP_PATH = "";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<ExprType, BiFunction<Content, ExprType, ExprValue>> typeActionMap =
      new ImmutableMap.Builder<ExprType, BiFunction<Content, ExprType, ExprValue>>()
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Integer),
              (c, dt) -> new ExprIntegerValue(c.intValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Long),
              (c, dt) -> new ExprLongValue(c.longValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Short),
              (c, dt) -> new ExprShortValue(c.shortValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Byte),
              (c, dt) -> new ExprByteValue(c.byteValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Float),
              (c, dt) -> new ExprFloatValue(c.floatValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Double),
              (c, dt) -> new ExprDoubleValue(c.doubleValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Text),
              (c, dt) -> new OpenSearchExprTextValue(c.stringValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword),
              (c, dt) -> new ExprStringValue(c.stringValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Boolean),
              (c, dt) -> ExprBooleanValue.of(c.booleanValue()))
          //Handles the creation of DATE, TIME, TIMESTAMP
          .put(OpenSearchDataType.of(ExprCoreType.TIMESTAMP),
              (c, dt) -> parseTimestamp(c, dt))
          .put(OpenSearchDataType.of(ExprCoreType.TIME),
              (c, dt) -> parseTimestamp(c, dt))
          .put(OpenSearchDataType.of(ExprCoreType.DATETIME),
              (c, dt) -> parseTimestamp(c, dt))
          .put(OpenSearchDataType.of(ExprCoreType.DATE),
              (c, dt) -> parseTimestamp(c, dt))
          .put(OpenSearchDateType.create(""),
              (c, dt) -> parseTimestamp(c, dt))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Ip),
              (c, dt) -> new OpenSearchExprIpValue(c.stringValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.GeoPoint),
              (c, dt) -> new OpenSearchExprGeoPointValue(c.geoValue().getLeft(),
                  c.geoValue().getRight()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Binary),
              (c, dt) -> new OpenSearchExprBinaryValue(c.stringValue()))
          .build();

  /**
   * Constructor of OpenSearchExprValueFactory.
   */
  public OpenSearchExprValueFactory(Map<String, OpenSearchDataType> typeMapping) {
    this.typeMapping = OpenSearchDataType.traverseAndFlatten(typeMapping);
  }

  /**
   * The struct construction has the following assumption:
   *  1. The field has OpenSearch Object data type.
   *     See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/object.html">
   *       docs</a>
   *  2. The deeper field is flattened in the typeMapping. e.g.
   *     { "employ",       "STRUCT"  }
   *     { "employ.id",    "INTEGER" }
   *     { "employ.state", "STRING"  }
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
    if (type.equals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Object))
          || type == STRUCT) {
      return parseStruct(content, field);
    } else if (type.equals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Nested))) {
      return parseArray(content, field);
    } else {
      if (typeActionMap.containsKey(type)) {
        return typeActionMap.get(type).apply(content, type);
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
   * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/date/#formats">
   *   docs</a>
   * The customized date_format is not supported.
   */
  private ExprValue constructTimestamp(String value) {
    try {
      return new ExprTimestampValue(
          // Using OpenSearch DateFormatters for now.
          DateFormatters.from(DATE_TIME_FORMATTER.parse(value)).toInstant());
    } catch (DateTimeParseException e) {
      throw new IllegalStateException(
          String.format(
              "Construct ExprTimestampValue from \"%s\" failed, unsupported date format.", value),
          e);
    }
  }

  private TemporalAccessor parseTimestampString(String value, OpenSearchDateType dt) {
    for (DateFormatter formatter : dt.getNamedFormatters()) {
      try {
        return formatter.parse(value);
      } catch (IllegalArgumentException  ignored) {
        // nothing to do, try another format
      }
    }
    return null;
  }

  private ExprValue formatReturn(ExprType formatType, ExprTimestampValue unformatted) {
    if (formatType.equals(ExprCoreType.DATE)) {
      return new ExprDateValue(unformatted.dateValue());
    }
    if (formatType.equals(ExprCoreType.DATETIME)) {
      return new ExprDatetimeValue(unformatted.datetimeValue());
    }
    if (formatType.equals(ExprCoreType.TIME)) {
      return new ExprTimeValue(unformatted.timeValue().toString());
    }
    return unformatted;
  }

  private ExprValue parseTimestamp(Content value, ExprType type) {
    OpenSearchDateType dt;
    ExprType returnFormat;
    if (type instanceof OpenSearchDateType) {
      //Case when an OpenSearchDateType is passed in
      dt = (OpenSearchDateType) type;
      returnFormat = dt.getExprType();
    } else {
      //Case when an OpenSearchDataType.of(<ExprCoreType>) is passed in
      dt = OpenSearchDateType.of();
      returnFormat = ((OpenSearchDataType) type).getExprType();
    }

    if (value.isNumber()) {
      return formatReturn(
          returnFormat,
          new ExprTimestampValue(Instant.ofEpochMilli(value.longValue())));
    }

    if (value.isString()) {
      TemporalAccessor parsed = parseTimestampString(value.stringValue(), dt);
      if (parsed == null) { // failed to parse or no formats given
        return formatReturn(
            returnFormat,
            (ExprTimestampValue) constructTimestamp(value.stringValue()));
      }
      // Try Timestamp
      try {
        return formatReturn(returnFormat, new ExprTimestampValue(Instant.from(parsed)));
      } catch (DateTimeException ignored) {
        // nothing to do, try another type
      }
      return constructTimestamp(value.stringValue());
    }
    return new ExprTimestampValue((Instant) value.objectValue());
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
   * Todo. ARRAY is not completely supported now. In OpenSearch, there is no dedicated array type.
   * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/nested/">docs</a>
   * The similar data type is nested, but it can only allow a list of objects.
   */
  private ExprValue parseArray(Content content, String prefix) {
    List<ExprValue> result = new ArrayList<>();
    // ExprCoreType.ARRAY does not indicate inner elements type.
    if (Iterators.size(content.array()) == 1 && content.objectValue() instanceof JsonNode) {
      result.add(parse(content, prefix, Optional.of(STRUCT)));
    } else {
      content.array().forEachRemaining(v -> {
        // ExprCoreType.ARRAY does not indicate inner elements type. OpenSearch nested will be an
        // array of structs, otherwise parseArray currently only supports array of strings.
        if (v.isString()) {
          result.add(parse(v, prefix, Optional.of(OpenSearchDataType.of(STRING))));
        } else {
          result.add(parse(v, prefix, Optional.of(STRUCT)));
        }
      });
    }
    return new ExprCollectionValue(result);
  }

  private String makeField(String path, String field) {
    return path.equalsIgnoreCase(TOP_PATH) ? field : String.join(".", path, field);
  }
}
