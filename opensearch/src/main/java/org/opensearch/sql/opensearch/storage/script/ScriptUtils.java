/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.type.ExprType;

/**
 * Script Utils.
 */
@UtilityClass
public class ScriptUtils {

  /**
   * Text field doesn't have doc value (exception thrown even when you call "get")
   * Limitation: assume inner field name is always "keyword".
   */
  public static String convertTextToKeyword(String fieldName, ExprType fieldType) {
    // TODO
    return fieldName;
  }
}
