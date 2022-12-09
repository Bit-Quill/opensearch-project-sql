/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.operator.arthmetic;

import java.math.BigDecimal;
import java.math.RoundingMode;
import lombok.experimental.UtilityClass;

@UtilityClass
public class MathUtil {

  /**
   * Truncates a number to required decimal places.
   *
   * @param numberToTruncate number to be truncated
   * @param numberOfDecimals required decimal places
   * @return truncated number as {@link BigDecimal}
   */
  public static BigDecimal truncateNumber(double numberToTruncate, int numberOfDecimals) {
    if (numberToTruncate > 0) {
      return new BigDecimal(String.valueOf(numberToTruncate))
                  .setScale(numberOfDecimals, RoundingMode.FLOOR);
    } else {
      return new BigDecimal(String
                .valueOf(numberToTruncate)).setScale(numberOfDecimals, RoundingMode.CEILING);
    }
  }
}
