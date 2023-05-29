/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.opensearch.common.time.FormatNames.ISO8601;
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
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_DATETIME_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_DATE_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_TIME_FORMATS;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.FormatNames;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchDateTypeTest {
  private static final String formatString = "epoch_millis || yyyyMMDD";
  private static final String defaultFormatString = "";

  private static final String dateFormatString = "date";

  private static final String timeFormatString = "hourMinuteSecond";

  private static final String datetimeFormatString = "basic_date_time";

  private static final OpenSearchDateType defaultDateType =
      OpenSearchDateType.create(defaultFormatString);
  private static final OpenSearchDateType dateDateType =
      OpenSearchDateType.create(dateFormatString);
  private static final OpenSearchDateType timeDateType =
      OpenSearchDateType.create(timeFormatString);
  private static final OpenSearchDateType datetimeDateType =
      OpenSearchDateType.create(datetimeFormatString);

  @Test
  public void isCompatible() {
    // timestamp types is compatible with all date-types
    assertTrue(TIMESTAMP.isCompatible(defaultDateType));
    assertTrue(TIMESTAMP.isCompatible(dateDateType));
    assertTrue(TIMESTAMP.isCompatible(timeDateType));
    assertTrue(TIMESTAMP.isCompatible(datetimeDateType));

    // datetime
    assertFalse(DATETIME.isCompatible(defaultDateType));
    assertTrue(DATETIME.isCompatible(dateDateType));
    assertTrue(DATETIME.isCompatible(timeDateType));
    assertFalse(DATETIME.isCompatible(datetimeDateType));

    // time type
    assertFalse(TIME.isCompatible(defaultDateType));
    assertFalse(TIME.isCompatible(dateDateType));
    assertTrue(TIME.isCompatible(timeDateType));
    assertFalse(TIME.isCompatible(datetimeDateType));

    // date type
    assertFalse(DATE.isCompatible(defaultDateType));
    assertTrue(DATE.isCompatible(dateDateType));
    assertFalse(DATE.isCompatible(timeDateType));
    assertFalse(DATE.isCompatible(datetimeDateType));
  }

  // `typeName` and `legacyTypeName` return the same thing for date objects:
  // https://github.com/opensearch-project/sql/issues/1296
  @Test
  public void check_typeName() {
    // always use the MappingType of "DATE"
    assertEquals("DATE", defaultDateType.typeName());
    assertEquals("DATE", timeDateType.typeName());
    assertEquals("DATE", dateDateType.typeName());
    assertEquals("DATE", datetimeDateType.typeName());
  }

  @Test
  public void check_legacyTypeName() {
    // always use the legacy "DATE" type
    assertEquals("DATE", defaultDateType.legacyTypeName());
    assertEquals("DATE", timeDateType.legacyTypeName());
    assertEquals("DATE", dateDateType.legacyTypeName());
    assertEquals("DATE", datetimeDateType.legacyTypeName());
  }

  @Test
  public void check_exprTypeName() {
    // exprType changes based on type (no datetime):
    assertEquals(TIMESTAMP, defaultDateType.getExprType());
    assertEquals(TIME, timeDateType.getExprType());
    assertEquals(DATE, dateDateType.getExprType());
    assertEquals(TIMESTAMP, datetimeDateType.getExprType());
  }

  @Test
  public void checkSupportedFormatNamesCoverage() {
    EnumSet<FormatNames> allFormatNames = EnumSet.allOf(FormatNames.class);
    allFormatNames.stream().forEach(formatName -> {
      assertTrue(
          SUPPORTED_NAMED_DATETIME_FORMATS.contains(formatName)
              || SUPPORTED_NAMED_DATE_FORMATS.contains(formatName)
              || SUPPORTED_NAMED_TIME_FORMATS.contains(formatName),
          formatName + " not supported");
    });
  }

  @Test
  public void checkTimestampFormatNames() {
    SUPPORTED_NAMED_DATETIME_FORMATS.stream().forEach(
        datetimeFormat -> {
          String camelCaseName = datetimeFormat.getCamelCaseName();
          if (camelCaseName != null && !camelCaseName.isEmpty()) {
            OpenSearchDateType dateType =
                OpenSearchDateType.create(camelCaseName);
            assertTrue(dateType.getExprType() == TIMESTAMP, camelCaseName
                    + " does not format to a TIMESTAMP type, instead got "
                    + dateType.getExprType());
          }

          String snakeCaseName = datetimeFormat.getSnakeCaseName();
          if (snakeCaseName != null && !snakeCaseName.isEmpty()) {
            OpenSearchDateType dateType = OpenSearchDateType.create(snakeCaseName);
            assertTrue(dateType.getExprType() == TIMESTAMP, snakeCaseName
                + " does not format to a TIMESTAMP type, instead got "
                + dateType.getExprType());
          }
        }
    );

    // check the default format case
    OpenSearchDateType dateType = OpenSearchDateType.create("");
    assertTrue(dateType.getExprType() == TIMESTAMP);
  }

  @Test
  public void checkDateFormatNames() {
    SUPPORTED_NAMED_DATE_FORMATS.stream().forEach(
        dateFormat -> {
          String camelCaseName = dateFormat.getCamelCaseName();
          if (camelCaseName != null && !camelCaseName.isEmpty()) {
            OpenSearchDateType dateType =
                OpenSearchDateType.create(camelCaseName);
            assertTrue(dateType.getExprType() == DATE, camelCaseName
                + " does not format to a DATE type, instead got "
                + dateType.getExprType());
          }

          String snakeCaseName = dateFormat.getSnakeCaseName();
          if (snakeCaseName != null && !snakeCaseName.isEmpty()) {
            OpenSearchDateType dateType = OpenSearchDateType.create(snakeCaseName);
            assertTrue(dateType.getExprType() == DATE, snakeCaseName
                + " does not format to a DATE type, instead got "
                + dateType.getExprType());
          }
        }
    );
  }

  @Test
  public void checkTimeFormatNames() {
    SUPPORTED_NAMED_TIME_FORMATS.stream().forEach(
        timeFormat -> {
          String camelCaseName = timeFormat.getCamelCaseName();
          if (camelCaseName != null && !camelCaseName.isEmpty()) {
            OpenSearchDateType dateType =
                OpenSearchDateType.create(camelCaseName);
            assertTrue(dateType.getExprType() == TIME, camelCaseName
                + " does not format to a TIME type, instead got "
                + dateType.getExprType());
          }

          String snakeCaseName = timeFormat.getSnakeCaseName();
          if (snakeCaseName != null && !snakeCaseName.isEmpty()) {
            OpenSearchDateType dateType = OpenSearchDateType.create(snakeCaseName);
            assertTrue(dateType.getExprType() == TIME, snakeCaseName
                + " does not format to a TIME type, instead got "
                + dateType.getExprType());
          }
        }
    );
  }
}
