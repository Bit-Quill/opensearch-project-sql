/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.planner.logical;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.prometheus.planner.logical.rules.MergeAggAndIndexScan;
import org.opensearch.sql.prometheus.planner.logical.rules.MergeAggAndRelation;
import org.opensearch.sql.prometheus.planner.logical.rules.MergeFilterAndRelation;

import java.util.List;

/**
 * Prometheus storage engine specified logical plan optimizer.
 */
@UtilityClass
public class PrometheusLogicalPlanOptimizerFactory {

  /**
   * Create Prometheus storage specified logical plan optimizer.
   */
  public static LogicalPlanOptimizer create() {
    return new LogicalPlanOptimizer(List.of(
        new MergeFilterAndRelation(),
        new MergeAggAndIndexScan(),
        new MergeAggAndRelation()
    ), LogicalPlanOptimizer.OptimizingMode.PRESERVE_TREE_ORDER);
  }
}
