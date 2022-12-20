/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
public class OpenSearchBinaryType extends OpenSearchDataType {

  public OpenSearchBinaryType() {
    super(Type.Binary);
    exprCoreType = UNKNOWN;
  }
}
