/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.storage.StorageEngine;

class PaginatedPlanCacheTest {

  @Mock
  StorageEngine storageEngine;

  PaginatedPlanCache planCache;
  @BeforeEach
  void setUp() {
    planCache = new PaginatedPlanCache(storageEngine);
  }

  @Test
  void canConvertToCursor_relation() {
    Assertions.assertTrue(planCache.canConvertToCursor(AstDSL.relation("Table")));
  }

  @Test
  void canConvertToCursor_project_allFields_relation() {
    var unresolvedPlan = AstDSL.project(AstDSL.relation("table"), AstDSL.allFields());
    Assertions.assertTrue(planCache.canConvertToCursor(unresolvedPlan));
  }

  @Test
  void canConvertToCursor_project_some_fields_relation() {
    var unresolvedPlan = AstDSL.project(AstDSL.relation("table"), AstDSL.field("rando"));
    Assertions.assertFalse(planCache.canConvertToCursor(unresolvedPlan));
  }
}
