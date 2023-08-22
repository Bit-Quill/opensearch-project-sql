/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.ScoreFunction;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.OpenSearchFunction;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlan;

@ExtendWith(MockitoExtension.class)
class OpenSearchAnalyzerTest extends AnalyzerTestBase {

  @Mock
  private BuiltinFunctionRepository builtinFunctionRepository;

  @Override
  protected ExpressionAnalyzer expressionAnalyzer() {
    return new ExpressionAnalyzer(builtinFunctionRepository);
  }

  @BeforeEach
  private void setup() {
    this.expressionAnalyzer = expressionAnalyzer();
    this.analyzer = analyzer(this.expressionAnalyzer, dataSourceService);
  }

  @Test
  public void analyze_filter_visit_score_function() {

    // setup
    OpenSearchFunction scoreFunction = new OpenSearchFunction(
        new FunctionName("match_phrase_prefix"), List.of());
    when(builtinFunctionRepository.compile(any(), any(), any())).thenReturn(scoreFunction);

    UnresolvedPlan unresolvedPlan =
        AstDSL.filter(
            AstDSL.relation("schema"),
            new ScoreFunction(
                AstDSL.function(
                    "match_phrase_prefix",
                    AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
                    AstDSL.unresolvedArg("query", stringLiteral("search query")),
                    AstDSL.unresolvedArg("boost", stringLiteral("3"))),
                AstDSL.doubleLiteral(1.0)));

    // test
    LogicalPlan logicalPlan = analyze(unresolvedPlan);
    OpenSearchFunction relevanceQuery =
        (OpenSearchFunction) ((LogicalFilter) logicalPlan).getCondition();

    // verify
    assertEquals(true, relevanceQuery.isScoreTracked());
  }

  @Test
  public void analyze_filter_visit_score_function_with_unsupported_boost_SemanticCheckException() {

    // setup
    UnresolvedPlan unresolvedPlan =
        AstDSL.filter(
            AstDSL.relation("schema"),
            new ScoreFunction(
                AstDSL.function(
                    "match_phrase_prefix",
                    AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
                    AstDSL.unresolvedArg("query", stringLiteral("search query")),
                    AstDSL.unresolvedArg("boost", stringLiteral("3"))),
                AstDSL.stringLiteral("3.0")));

    // Test
    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> analyze(unresolvedPlan));

    // Verify
    assertEquals("Expected boost type 'DOUBLE' but got 'STRING'", exception.getMessage());
  }
}
