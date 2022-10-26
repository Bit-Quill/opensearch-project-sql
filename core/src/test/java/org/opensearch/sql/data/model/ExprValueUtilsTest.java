/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.data.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.model.ExprValueUtils.convertEpochMilliToDateTimeType;
import static org.opensearch.sql.data.model.ExprValueUtils.extractEpochMilliFromAnyDateTimeType;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

@DisplayName("Test Expression Value Utils")
public class ExprValueUtilsTest {
  private static LinkedHashMap<String, ExprValue> testTuple = new LinkedHashMap<>();

  static {
    testTuple.put("1", new ExprIntegerValue(1));
  }

  private static List<ExprValue> numberValues = Stream.of((byte) 1, (short) 1, 1, 1L, 1f, 1D)
      .map(ExprValueUtils::fromObjectValue).collect(Collectors.toList());

  private static List<ExprValue> nonNumberValues = Arrays.asList(
      new ExprStringValue("1"),
      ExprBooleanValue.of(true),
      new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(1))),
      new ExprTupleValue(testTuple),
      new ExprDateValue("2012-08-07"),
      new ExprTimeValue("18:00:00"),
      new ExprDatetimeValue("2012-08-07 18:00:00"),
      new ExprTimestampValue("2012-08-07 18:00:00"),
      new ExprIntervalValue(Duration.ofSeconds(100)));

  private static List<ExprValue> allValues =
      Lists.newArrayList(Iterables.concat(numberValues, nonNumberValues));

  private static List<Function<ExprValue, Object>> numberValueExtractor = Arrays.asList(
      ExprValueUtils::getByteValue,
      ExprValueUtils::getShortValue,
      ExprValueUtils::getIntegerValue,
      ExprValueUtils::getLongValue,
      ExprValueUtils::getFloatValue,
      ExprValueUtils::getDoubleValue);
  private static List<Function<ExprValue, Object>> nonNumberValueExtractor = Arrays.asList(
      ExprValueUtils::getStringValue,
      ExprValueUtils::getBooleanValue,
      ExprValueUtils::getCollectionValue,
      ExprValueUtils::getTupleValue
  );
  private static List<Function<ExprValue, Object>> dateAndTimeValueExtractor = Arrays.asList(
      ExprValue::dateValue,
      ExprValue::timeValue,
      ExprValue::datetimeValue,
      ExprValue::timestampValue,
      ExprValue::intervalValue);
  private static List<Function<ExprValue, Object>> allValueExtractor = Lists.newArrayList(
      Iterables.concat(numberValueExtractor, nonNumberValueExtractor, dateAndTimeValueExtractor));

  private static List<ExprCoreType> numberTypes =
      Arrays.asList(ExprCoreType.BYTE, ExprCoreType.SHORT, ExprCoreType.INTEGER, ExprCoreType.LONG,
          ExprCoreType.FLOAT, ExprCoreType.DOUBLE);
  private static List<ExprCoreType> nonNumberTypes =
      Arrays.asList(STRING, BOOLEAN, ARRAY, STRUCT);
  private static List<ExprCoreType> dateAndTimeTypes =
      Arrays.asList(DATE, TIME, DATETIME, TIMESTAMP, INTERVAL);
  private static List<ExprCoreType> allTypes =
      Lists.newArrayList(Iterables.concat(numberTypes, nonNumberTypes, dateAndTimeTypes));

  private static Stream<Arguments> getValueTestArgumentStream() {
    List<Object> expectedValues = Arrays.asList((byte) 1, (short) 1, 1, 1L, 1f, 1D, "1", true,
        Arrays.asList(integerValue(1)),
        ImmutableMap.of("1", integerValue(1)),
        LocalDate.parse("2012-08-07"),
        LocalTime.parse("18:00:00"),
        LocalDateTime.parse("2012-08-07T18:00:00"),
        ZonedDateTime.of(LocalDateTime.parse("2012-08-07T18:00:00"), ZoneId.of("UTC")).toInstant(),
        Duration.ofSeconds(100)
    );
    Stream.Builder<Arguments> builder = Stream.builder();
    for (int i = 0; i < expectedValues.size(); i++) {
      builder.add(Arguments.of(
          allValues.get(i),
          allValueExtractor.get(i),
          expectedValues.get(i)));
    }
    return builder.build();
  }

  private static Stream<Arguments> getTypeTestArgumentStream() {
    Stream.Builder<Arguments> builder = Stream.builder();
    for (int i = 0; i < allValues.size(); i++) {
      builder.add(Arguments.of(
          allValues.get(i),
          allTypes.get(i)));
    }
    return builder.build();
  }

  private static Stream<Arguments> invalidGetNumberValueArgumentStream() {
    return Lists.cartesianProduct(nonNumberValues, numberValueExtractor)
        .stream()
        .map(list -> Arguments.of(list.get(0), list.get(1)));
  }

  @SuppressWarnings("unchecked")
  private static Stream<Arguments> invalidConvert() {
    List<Map.Entry<Function<ExprValue, Object>, ExprCoreType>> extractorWithTypeList =
        new ArrayList<>();
    for (int i = 0; i < nonNumberValueExtractor.size(); i++) {
      extractorWithTypeList.add(
          new AbstractMap.SimpleEntry<>(nonNumberValueExtractor.get(i), nonNumberTypes.get(i)));
    }
    return Lists.cartesianProduct(allValues, extractorWithTypeList)
        .stream()
        .filter(list -> {
          ExprValue value = (ExprValue) list.get(0);
          Map.Entry<Function<ExprValue, Object>, ExprCoreType> entry =
              (Map.Entry<Function<ExprValue, Object>,
                  ExprCoreType>) list
                  .get(1);
          return entry.getValue() != value.type();
        })
        .map(list -> {
          Map.Entry<Function<ExprValue, Object>, ExprCoreType> entry =
              (Map.Entry<Function<ExprValue, Object>,
                  ExprCoreType>) list
                  .get(1);
          return Arguments.of(list.get(0), entry.getKey(), entry.getValue());
        });
  }

  @ParameterizedTest(name = "the value of ExprValue:{0} is: {2} ")
  @MethodSource("getValueTestArgumentStream")
  public void getValue(ExprValue value, Function<ExprValue, Object> extractor, Object expect) {
    assertEquals(expect, extractor.apply(value));
  }

  @ParameterizedTest(name = "the type of ExprValue:{0} is: {1} ")
  @MethodSource("getTypeTestArgumentStream")
  public void getType(ExprValue value, ExprCoreType expectType) {
    assertEquals(expectType, value.type());
  }

  /**
   * Test Invalid to get number.
   */
  @ParameterizedTest(name = "invalid to get number value of ExprValue:{0}")
  @MethodSource("invalidGetNumberValueArgumentStream")
  public void invalidGetNumberValue(ExprValue value, Function<ExprValue, Object> extractor) {
    Exception exception = assertThrows(ExpressionEvaluationException.class,
        () -> extractor.apply(value));
    assertThat(exception.getMessage(), Matchers.containsString("invalid"));
  }

  /**
   * Test Invalid to convert.
   */
  @ParameterizedTest(name = "invalid convert ExprValue:{0} to ExprType:{2}")
  @MethodSource("invalidConvert")
  public void invalidConvertExprValue(ExprValue value, Function<ExprValue, Object> extractor,
                                      ExprCoreType toType) {
    Exception exception = assertThrows(ExpressionEvaluationException.class,
        () -> extractor.apply(value));
    assertThat(exception.getMessage(), Matchers.containsString("invalid"));
  }

  @Test
  public void unSupportedObject() {
    Exception exception = assertThrows(ExpressionEvaluationException.class,
        () -> ExprValueUtils.fromObjectValue(integerValue(1)));
    assertEquals(
        "unsupported object "
            + "class org.opensearch.sql.data.model.ExprIntegerValue",
        exception.getMessage());
  }

  @Test
  public void bindingTuples() {
    for (ExprValue value : allValues) {
      if (STRUCT == value.type()) {
        assertNotEquals(BindingTuple.EMPTY, value.bindingTuples());
      } else {
        assertEquals(BindingTuple.EMPTY, value.bindingTuples());
      }
    }
  }

  @Test
  public void constructDateAndTimeValue() {
    assertEquals(new ExprDateValue("2012-07-07"),
        ExprValueUtils.fromObjectValue("2012-07-07", DATE));
    assertEquals(new ExprTimeValue("01:01:01"),
        ExprValueUtils.fromObjectValue("01:01:01", TIME));
    assertEquals(new ExprDatetimeValue("2012-07-07 01:01:01"),
        ExprValueUtils.fromObjectValue("2012-07-07 01:01:01", DATETIME));
    assertEquals(new ExprTimestampValue("2012-07-07 01:01:01"),
        ExprValueUtils.fromObjectValue("2012-07-07 01:01:01", TIMESTAMP));
  }

  @Test
  public void hashCodeTest() {
    assertEquals(new ExprByteValue(1).hashCode(), new ExprByteValue(1).hashCode());
    assertEquals(new ExprShortValue(1).hashCode(), new ExprShortValue(1).hashCode());
    assertEquals(new ExprIntegerValue(1).hashCode(), new ExprIntegerValue(1).hashCode());
    assertEquals(new ExprStringValue("1").hashCode(), new ExprStringValue("1").hashCode());
    assertEquals(new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(1))).hashCode(),
        new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(1))).hashCode());
    assertEquals(new ExprTupleValue(testTuple).hashCode(),
        new ExprTupleValue(testTuple).hashCode());
    assertEquals(new ExprDateValue("2012-08-07").hashCode(),
        new ExprDateValue("2012-08-07").hashCode());
    assertEquals(new ExprTimeValue("18:00:00").hashCode(),
        new ExprTimeValue("18:00:00").hashCode());
    assertEquals(new ExprDatetimeValue("2012-08-07 18:00:00").hashCode(),
        new ExprDatetimeValue("2012-08-07 18:00:00").hashCode());
    assertEquals(new ExprTimestampValue("2012-08-07 18:00:00").hashCode(),
        new ExprTimestampValue("2012-08-07 18:00:00").hashCode());
  }

  private static Stream<Arguments> getMillisForConversionTest() {
    return Stream.of(
      Arguments.of(42L),
      Arguments.of(-12345442000L),
      Arguments.of(100500L),
      Arguments.of(123456789L)
    );
  }

  /**
   * Check that `DATETIME` and `TIMESTAMP` could be converted to and from milliseconds since Epoch.
   * @param sample A test value (milliseconds since Epoch).
   */
  @ParameterizedTest
  @MethodSource("getMillisForConversionTest")
  public void checkDateTimeConversionToMillisAndBack(long sample) {
    for (var type : List.of(DATETIME, TIMESTAMP)) {
      var value = convertEpochMilliToDateTimeType(sample, type);
      assertEquals(type, value.type());
      var extracted = extractEpochMilliFromAnyDateTimeType(value);
      assertEquals(sample, extracted, type.toString());
    }
  }

  private final long millisInDay = 24 * 60 * 60 * 1000;

  /**
   * Check that `TIME` could be converted to and from milliseconds since Epoch.
   * @param sample A test value (milliseconds since Epoch).
   */
  @ParameterizedTest
  @MethodSource("getMillisForConversionTest")
  public void checkTimeConversionToMillisAndBack(long sample) {
    var value = convertEpochMilliToDateTimeType(sample, TIME);
    assertEquals(TIME, value.type());
    var extracted = extractEpochMilliFromAnyDateTimeType(value);
    // time value goes around 24h, for negative (pre-epoch) values we need to shift down one day.
    if (sample < 0) {
      assertEquals((sample % millisInDay) + millisInDay, extracted, TIME.toString());
    } else {
      assertEquals(sample % millisInDay, extracted, TIME.toString());
    }
  }

  /**
   * Check that `DATE` could be converted to and from milliseconds since Epoch.
   * @param sample A test value (milliseconds since Epoch).
   */
  @ParameterizedTest
  @MethodSource("getMillisForConversionTest")
  public void checkDateConversionToMillisAndBack(long sample) {
    var value = convertEpochMilliToDateTimeType(sample, DATE);
    assertEquals(DATE, value.type());
    var extracted = extractEpochMilliFromAnyDateTimeType(value);
    // date value floored by 24h, for negative (pre-epoch) values we need to shift down one day.
    if (sample < 0) {
      assertEquals((sample - millisInDay) / millisInDay * millisInDay, extracted, DATE.toString());
    } else {
      assertEquals((sample / millisInDay) * millisInDay, extracted, DATE.toString());
    }
  }

  /**
   * Check that conversion function reject all non-datetime types.
   * @param sample A test value (milliseconds since Epoch).
   */
  @ParameterizedTest
  @MethodSource("getMillisForConversionTest")
  public void checkExceptionThrownOnUnsupportedTypeConversion(long sample) {
    var types = ExprCoreType.coreTypes();
    types.removeAll(List.of(DATE, DATETIME, TIMESTAMP, TIME));
    for (var type : types) {
      var exception = assertThrows(IllegalArgumentException.class,
          () -> convertEpochMilliToDateTimeType(sample, type));
      assertEquals(String.format("Not a datetime type: %s", type), exception.getMessage());
    }
  }

  private static Stream<Arguments> getNonDateTimeValues() {
    var types = List.of(DATE, DATETIME, TIMESTAMP, TIME);
    return getValueTestArgumentStream()
        .filter(args -> !types.contains(((ExprValue)args.get()[0]).type()))
        .map(args -> Arguments.of(args.get()[0]));
  }

  /**
   * Check that conversion function reject all non-datetime types.
   * @param value A test value.
   */
  @ParameterizedTest(name = "the value of ExprValue:{0} is: {2} ")
  @MethodSource("getNonDateTimeValues")
  public void checkExceptionThrownOnUnsupportedTypeExtraction(ExprValue value) {
    var exception = assertThrows(IllegalArgumentException.class,
        () -> extractEpochMilliFromAnyDateTimeType(value));
    assertEquals(String.format("Not a datetime type: %s", value.type()), exception.getMessage());
  }
}
