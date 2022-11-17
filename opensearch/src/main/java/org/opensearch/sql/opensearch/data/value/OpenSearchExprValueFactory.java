/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_BINARY;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_GEO_POINT;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_IP;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
import org.apache.logging.log4j.LogManager;
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
import org.opensearch.sql.opensearch.data.utils.Content;
import org.opensearch.sql.opensearch.data.utils.ObjectContent;
import org.opensearch.sql.opensearch.data.utils.OpenSearchJsonContent;
import org.opensearch.sql.opensearch.mapping.MappingEntry;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

/**
 * Construct ExprValue from OpenSearch response.
 */
public class OpenSearchExprValueFactory {
  /**
   * The Mapping of Field and ExprType.
   */
  @Setter
  private Map<String, MappingEntry> typeMapping;

  @Getter
  @Setter
  private OpenSearchAggregationResponseParser parser;

  private static final String TOP_PATH = "";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<ExprType, BiFunction<Content, MappingEntry, ExprValue>> typeActionMap =
      new ImmutableMap.Builder<ExprType, BiFunction<Content, MappingEntry, ExprValue>>()
          .put(INTEGER, (c, m) -> new ExprIntegerValue(c.intValue()))
          .put(LONG, (c, m) -> new ExprLongValue(c.longValue()))
          .put(SHORT, (c, m) -> new ExprShortValue(c.shortValue()))
          .put(BYTE, (c, m) -> new ExprByteValue(c.byteValue()))
          .put(FLOAT, (c, m) -> new ExprFloatValue(c.floatValue()))
          .put(DOUBLE, (c, m) -> new ExprDoubleValue(c.doubleValue()))
          .put(STRING, (c, m) -> new ExprStringValue(c.stringValue()))
          .put(BOOLEAN, (c, m) -> ExprBooleanValue.of(c.booleanValue()))
          .put(TIMESTAMP, this::parseTimestamp)
          .put(DATE, (c, m) -> new ExprDateValue(parseTimestamp(c, m).dateValue().toString()))
          .put(TIME, (c, m) -> new ExprTimeValue(parseTimestamp(c, m).timeValue().toString()))
          .put(DATETIME, (c, m) -> new ExprDatetimeValue(parseTimestamp(c, m).datetimeValue()))
          .put(OPENSEARCH_TEXT, (c, m) -> new OpenSearchExprTextValue(c.stringValue()))
          .put(OPENSEARCH_TEXT_KEYWORD, (c, m) -> new OpenSearchExprTextKeywordValue(c.stringValue()))
          .put(OPENSEARCH_IP, (c, m) -> new OpenSearchExprIpValue(c.stringValue()))
          .put(OPENSEARCH_GEO_POINT, (c, m) -> new OpenSearchExprGeoPointValue(c.geoValue().getLeft(),
              c.geoValue().getRight()))
          .put(OPENSEARCH_BINARY, (c, m) -> new OpenSearchExprBinaryValue(c.stringValue()))
          .build();

  /**
   * Constructor of OpenSearchExprValueFactory.
   */
  public OpenSearchExprValueFactory(Map<String, MappingEntry> typeMapping) {
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
        return typeActionMap.get(type).apply(content, typeMapping.getOrDefault(field, null));
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
    return Optional.ofNullable(typeMapping.get(field).getDataType());
  }

  /**
   * Only default strict_date_optional_time||epoch_millis is supported,
   * strict_date_optional_time_nanos||epoch_millis if field is date_nanos.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/date_nanos.html
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

  // returns java.time.format.Parsed
  private TemporalAccessor parseTimestampString(String value, MappingEntry mapping) {
    if (mapping == null) {
      return null;
    }
    for (var formatter : mapping.getRegularFormatters()) {
      try {
        return formatter.parse(value);
      } catch (Exception ignored) {
        // nothing to do, try another format
      }
    }
    for (var formatter : mapping.getNamedFormatters()) {
      try {
        return formatter.parse(value);
      } catch (Exception ignored) {
        // nothing to do, try another format
      }
    }
    return null;
  }

  private ExprValue parseTimestamp(Content value, MappingEntry mapping) {
    if (value.isNumber()) {
      return new ExprTimestampValue(Instant.ofEpochMilli(value.longValue()));
    } else if (value.isString()) {
      TemporalAccessor parsed = parseTimestampString(value.stringValue(), mapping);
      if (parsed == null) { // failed to parse or no formats given
        return constructTimestamp(value.stringValue());
      }
      try {
        return new ExprTimestampValue(Instant.from(parsed));
      } catch (DateTimeException ignored) {
        // nothing to do, try another type
      }
      // TODO return not ExprTimestampValue
      try {
        return new ExprTimestampValue(new ExprDateValue(LocalDate.from(parsed)).timestampValue());
      } catch (DateTimeException ignored) {
        // nothing to do, try another type
      }
      try {
        return new ExprTimestampValue(new ExprDatetimeValue(LocalDateTime.from(parsed)).timestampValue());
      } catch (DateTimeException ignored) {
        // nothing to do, try another type
      }
      try {
        return new ExprTimestampValue(new ExprTimeValue(LocalTime.from(parsed)).timestampValue());
      } catch (DateTimeException ignored) {
        // nothing to do, try another type
      }
      // TODO throw exception
      LogManager.getLogger(OpenSearchExprValueFactory.class).error(
          String.format("Can't recognize parsed value: %s, %s", parsed, parsed.getClass()));
      return new ExprStringValue(value.stringValue());
    } else {
      return new ExprTimestampValue((Instant) value.objectValue());
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
