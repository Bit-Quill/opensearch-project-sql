/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

public class TimeType implements TypeHelper<Time>{

  public static final TimeType INSTANCE = new TimeType();

  private TimeType() {

  }

  @Override
  public Time fromValue(Object value, Map<String, Object> conversionParams) throws SQLException {
    if (value == null) {
      return null;
    }
    Calendar calendar = conversionParams != null ? (Calendar) conversionParams.get("calendar") : null;
    if (value instanceof Time) {
      return (Time) value;
    } else if (value instanceof String) {
      return asTime((String) value, calendar);
    } else if (value instanceof Number) {
      return this.asTime((Number) value);
    } else {
      throw objectConversionException(value);
    }
  }

  public Time asTime(String value, Calendar calendar) throws SQLException {
    try {
      // Make some effort to understand ISO format
      if (value.length() > 11 && value.charAt(10) == 'T') {
        value = value.replace('T', ' ');
      }
      // Timestamp.valueOf() does not like timezone information
      if (value.length() > 23) {
        if (value.length() == 24 && value.charAt(23) == 'Z') {
          value = value.substring(0, 23);
        } else if (value.charAt(23) == '+' || value.charAt(23) == '-') {
          // 'calendar' parameter takes precedence
          if (calendar == null) {
            calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT" + value.substring(23)));
          }
          value = value.substring(0, 23);
        }
      }

      final Timestamp ts;
      // 11 to check if the value is in yyyy-MM-dd format
      if (value.length() < 11) {
        ts = Timestamp.valueOf(LocalDate.parse(value).atStartOfDay());
      } else {
        ts = Timestamp.valueOf(value);
      }

      if (calendar == null) {
        return new Time(ts.getTime());
      }
      return localDateTimeToTimestamp(ts.toLocalDateTime(), calendar);
    } catch (IllegalArgumentException iae) {
      throw stringConversionException(value, iae);
    }
  }

  public Time asTime(Number value) {
    return new Time(value.longValue());
  }

  @Override
  public String getTypeName() {
    return "Time";
  }

  private Time localDateTimeToTimestamp(LocalDateTime ldt, Calendar calendar) {
    calendar.set(ldt.getYear(), ldt.getMonthValue()-1, ldt.getDayOfMonth(),
            ldt.getHour(), ldt.getMinute(), ldt.getSecond());
    calendar.set(Calendar.MILLISECOND, ldt.getNano()/1000000);

    return new Time(new Timestamp(calendar.getTimeInMillis()).getTime());
  }
}
