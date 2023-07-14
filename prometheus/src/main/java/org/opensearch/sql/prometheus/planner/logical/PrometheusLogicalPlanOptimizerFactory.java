/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.planner.logical;

import com.google.common.collect.ImmutableList;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.optimizer.Rule;
import org.opensearch.sql.prometheus.planner.logical.rules.MergeAggAndIndexScan;
import org.opensearch.sql.prometheus.planner.logical.rules.MergeAggAndRelation;
import org.opensearch.sql.prometheus.planner.logical.rules.MergeFilterAndRelation;

/**
 * Prometheus storage engine specified logical plan optimizer.
 */
@UtilityClass
public class PrometheusLogicalPlanOptimizerFactory {

  /**
   * Create Prometheus storage specified logical plan optimizer.
   */
  public static LogicalPlanOptimizer create() {
    return new LogicalPlanOptimizer(
        new ImmutableList.Builder<Rule<? extends LogicalPlan>>()
            .add(new MergeFilterAndRelation())
            .add(new MergeAggAndIndexScan())
            .add(new MergeAggAndRelation())
            .build()
    );
  }
}
