/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.highlight;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Highlight;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ExpressionConfig.class, AnalyzerTestBase.class})
@ExtendWith(MockitoExtension.class)
public class LogicalHighlightTest extends AnalyzerTestBase {
  @Test
  public void analyze_highlight_with_one_field() {
    Map<String, Literal> args = new HashMap<>();
    assertAnalyzeEqual(
        LogicalPlanDSL.highlight(
            LogicalPlanDSL.relation("schema"),
            DSL.literal("field"), args, "highlight(field)"),
        new Highlight(
            alias("highlight('field')",
                highlight(stringLiteral("field"), args)),
            args, "highlight(field)")
            .attach(relation("schema")));
  }
}
