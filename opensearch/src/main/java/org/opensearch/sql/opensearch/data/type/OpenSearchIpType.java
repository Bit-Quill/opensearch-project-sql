/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import lombok.EqualsAndHashCode;

/**
 * The type of a binary value. See
 * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/ip/">doc</a>
 */
@EqualsAndHashCode(callSuper = false)
public class OpenSearchIpType extends OpenSearchDataType {

  public OpenSearchIpType() {
    super(Type.Ip);
    exprCoreType = UNKNOWN;
  }
}
