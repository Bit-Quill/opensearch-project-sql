/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.planner.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.aggregation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.filter;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.prometheus.utils.LogicalPlanUtils.indexScan;
import static org.opensearch.sql.prometheus.utils.LogicalPlanUtils.indexScanAgg;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.function.FunctionPropertiesTestConfig;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.storage.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {FunctionPropertiesTestConfig.class, ExpressionConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class PrometheusLogicOptimizerTest {

  @Autowired
  DSL dsl;

  @Mock
  private Table table;

  @Test
  void project_filter_merge_with_relation() {
    assertEquals(
        project(
            indexScan("prometheus_http_total_requests",
                dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))))
        ),
        optimize(
            project(
                filter(
                    relation("prometheus_http_total_requests", table),
                    dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200")))
                ))
        )
    );
  }

  @Test
  void aggregation_merge_relation() {
    assertEquals(
        project(
            indexScanAgg("prometheus_http_total_requests", ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(DSL.named("code", DSL.ref("code", STRING)))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))),
        optimize(
            project(
                aggregation(
                    relation("prometheus_http_total_requests", table),
                    ImmutableList
                        .of(DSL.named("AVG(@value)",
                            dsl.avg(DSL.ref("@value", INTEGER)))),
                    ImmutableList.of(DSL.named("code",
                        DSL.ref("code", STRING)))),
                DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE)))
        )
    );
  }


  @Test
  void aggregation_merge_filter_relation() {
    assertEquals(
        project(
            indexScanAgg("prometheus_http_total_requests",
                dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))),
                ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(DSL.named("job", DSL.ref("job", STRING)))),
            DSL.named("AVG(@value)", DSL.ref("AVG(@value)", DOUBLE))),
        optimize(
            project(
                aggregation(
                    filter(
                        relation("prometheus_http_total_requests", table),
                        dsl.and(
                            dsl.equal(DSL.ref("code", STRING),
                                DSL.literal(stringValue("200"))),
                            dsl.equal(DSL.ref("handler", STRING),
                                DSL.literal(stringValue("/ready/"))))
                    ),
                    ImmutableList
                        .of(DSL.named("AVG(@value)",
                            dsl.avg(DSL.ref("@value", INTEGER)))),
                    ImmutableList.of(DSL.named("job",
                        DSL.ref("job", STRING)))),
                DSL.named("AVG(@value)", DSL.ref("AVG(@value)", DOUBLE)))
        )
    );
  }


  private LogicalPlan optimize(LogicalPlan plan) {
    final LogicalPlanOptimizer optimizer = PrometheusLogicalPlanOptimizerFactory.create();
    return optimizer.optimize(plan);
  }

}
