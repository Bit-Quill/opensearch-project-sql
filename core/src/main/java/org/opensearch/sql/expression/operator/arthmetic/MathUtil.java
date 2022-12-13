/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.operator.arthmetic;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class MathUtil {

  /**
   * Truncates a double number to required decimal places.
   *
   * @param numberToTruncate number to be truncated
   * @param numberOfDecimals required decimal places
   * @return truncated number as double
   */
  public static double truncateDouble(double numberToTruncate, int numberOfDecimals) {
    return new BigDecimal(String.valueOf(numberToTruncate)).setScale(numberOfDecimals,
            numberToTruncate > 0 ? RoundingMode.FLOOR : RoundingMode.CEILING).doubleValue();
  }

  /**
   * Truncates a float number to required decimal places.
   *
   * @param numberToTruncate number to be truncated
   * @param numberOfDecimals required decimal places
   * @return truncated number as double
   */
  public static double truncateFloat(float numberToTruncate, int numberOfDecimals) {
    return new BigDecimal(String.valueOf(numberToTruncate)).setScale(numberOfDecimals,
            numberToTruncate > 0 ? RoundingMode.FLOOR : RoundingMode.CEILING).doubleValue();
  }

  /**
   * Truncates an int number to required decimal places.
   *
   * @param numberToTruncate number to be truncated
   * @param numberOfDecimals required decimal places
   * @return truncated number as long
   */
  public static long truncateInt(int numberToTruncate, int numberOfDecimals) {
    return new BigDecimal(String.valueOf(numberToTruncate)).setScale(numberOfDecimals,
            numberToTruncate > 0 ? RoundingMode.FLOOR : RoundingMode.CEILING).longValue();
  }

  /**
   * Truncates a long number to required decimal places.
   *
   * @param numberToTruncate number to be truncated
   * @param numberOfDecimals required decimal places
   * @return truncated number as long
   */
  public static long truncateLong(long numberToTruncate, int numberOfDecimals) {
    return new BigDecimal(String.valueOf(numberToTruncate)).setScale(numberOfDecimals,
            numberToTruncate > 0 ? RoundingMode.FLOOR : RoundingMode.CEILING).longValue();
  }
}
