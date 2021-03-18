package net.sansa_stack.inference.rules.plan

import com.google.common.collect.ImmutableSet
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalJoin, LogicalProject}
import org.apache.calcite.rex.RexNode

/**
  * @author Lorenz Buehmann
  */
class SimplePlanExecutor {

  def execute(plan: RelNode): Unit = {
//    plan match {
//      case LogicalProject => plan.asInstanceOf[LogicalProject].
//      case LogicalJoin =>
//      case LogicalFilter(cluster,
//      traitSet,
//      child,
//      condition,
//      variablesSet) =>
//    }
  }


}
