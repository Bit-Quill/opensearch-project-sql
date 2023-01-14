/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.exception;

/**
 * Exception thrown to fall back to legacy when nested fields are queried.
 */
public class UnsupportedV2NestedException extends QueryEngineException {
  public UnsupportedV2NestedException(String message) {
    super(message);
  }
}



