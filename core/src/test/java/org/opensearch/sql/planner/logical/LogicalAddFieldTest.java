/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.expression.DSL;

@ExtendWith(MockitoExtension.class)
public class LogicalAddFieldTest extends AnalyzerTestBase {
  @Test
  public void analyze_addfield_with_one_field() {
    assertAnalyzeEqual(
        LogicalPlanDSL.eval(
            LogicalPlanDSL.relation("schema", table),
            ImmutablePair.of(DSL.ref("x", STRING), DSL.literal("foo"))),
        AstDSL.addField(
            AstDSL.relation("schema"), AstDSL.stringLiteral("x"), AstDSL.stringLiteral("foo")));
  }
}
