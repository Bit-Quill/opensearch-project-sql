/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import lombok.Getter;
import lombok.Setter;

public class JsonSupportAnalysisContext {
    @Getter
    @Setter
    boolean isJSONSupported = true;
}
