/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import java.util.List;
import org.opensearch.sql.data.type.ExprType;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
public class OpenSearchTextType extends OpenSearchDataType {

  public OpenSearchTextType() {
    super(Type.Text);
    exprCoreType = UNKNOWN;
  }

  @Override
  public List<ExprType> getParent() {
    return List.of(STRING);
  }

  @Override
  public boolean shouldCast(ExprType other) {
    return false;
  }
}
