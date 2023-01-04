/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

/**
 * Expression Text Keyword Value, it is an extension of the ExprValue by OpenSearch.
 * This mostly represents a multi-field in OpenSearch which has a text field and a
 * keyword field inside to preserve the original text.
 */
public class OpenSearchExprTextKeywordValue extends ExprStringValue {

  public OpenSearchExprTextKeywordValue(String value) {
    super(value);
  }

  @Override
  public ExprType type() {
    return new OpenSearchTextType();
  }
}
