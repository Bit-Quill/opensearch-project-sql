/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.model.transport;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import java.io.IOException;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;

public class DeleteDataSourceActionRequest extends ActionRequest {

  @Getter
  private String dataSourceName;

  /** Constructor of DeleteDataSourceActionRequest from StreamInput. */
  public DeleteDataSourceActionRequest(StreamInput in) throws IOException {
    super(in);
  }

  public DeleteDataSourceActionRequest(String dataSourceName) {
    this.dataSourceName = dataSourceName;
  }

  @Override
  public ActionRequestValidationException validate() {
    if (StringUtils.isEmpty(this.dataSourceName)) {
      ActionRequestValidationException exception = new ActionRequestValidationException();
      exception
          .addValidationError("Datasource Name cannot be empty or null");
      return exception;
    } else if (this.dataSourceName.equals(DEFAULT_DATASOURCE_NAME)) {
      ActionRequestValidationException exception = new ActionRequestValidationException();
      exception
          .addValidationError(
              "Not allowed to delete datasource with name : " + DEFAULT_DATASOURCE_NAME);
      return exception;
    } else {
      return null;
    }
  }

}
