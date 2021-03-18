package net.sansa_stack.query.spark.binding.engine
import net.sansa_stack.query.spark.binding.engine.OpExecutorImpl.SYM_RDD_OF_DATASET
import net.sansa_stack.query.spark.ops.rdd.RddOfBindingOps
import net.sansa_stack.rdf.spark.model.rdd.RddOfDatasetOps
import org.aksw.jena_sparql_api.utils.QueryUtils
import org.apache.jena.query.Dataset
import org.apache.jena.sparql.algebra.{Op, OpAsQuery}
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.util.Symbol
import org.apache.spark.rdd.RDD

object OpExecutorImpl {
  val SYM_RDD_OF_DATASET = Symbol.create("urn:rddOfDataset")
}

class OpExecutorImpl(val execCxt: ExecutionContext)
  extends OpExecutor
{
  val dispatcher: ExecutionDispatch = new ExecutionDispatch(this)
  var level = 0

  def exec(op: Op, input: RDD[Binding]): RDD[Binding] = {
    level += 1
    val result = dispatcher.exec(op, input)
    level -= 1
    result
  }

  override def execute(op: OpProject, rdd: RDD[Binding]): RDD[Binding] =
    RddOfBindingOps.project(exec(op.getSubOp, rdd), op.getVars)
    // RddOfBindingOps.project(rdd, op.getVars)

  override def execute(op: OpGroup, rdd: RDD[Binding]): RDD[Binding] =
    RddOfBindingOps.group(exec(op.getSubOp, rdd), op.getGroupVars, op.getAggregators)
//    RddOfBindingOps.group(rdd, op.getGroupVars, op.getAggregators)

  override def execute(op: OpService, rdd: RDD[Binding]): RDD[Binding] = {
    var result: RDD[Binding] = null

    val serviceNode = op.getService

    var success = false
    if (serviceNode.isURI) {
      val serviceUri = serviceNode.getURI

      // TODO Add some registry
      // TODO Consider deprecation and/or removalof rdd:perPartition because of the scalability issues when loading them into RAM
      if (serviceUri == "rdd:perPartition") {
        // Get the RDD[Dataset] from the execution context
        val rddOfDataset: RDD[Dataset] = execCxt.getContext.get(SYM_RDD_OF_DATASET)

        if (rddOfDataset == null) {
          throw new RuntimeException("No rddOfDataset in execution context - cannot delegate to " + serviceUri)
        }

        val query = if (op.getServiceElement != null) {
          QueryUtils.elementToQuery(op.getServiceElement)
        } else {
          OpAsQuery.asQuery(op.getSubOp)
        }

        result = RddOfDatasetOps.selectWithSparqlPerPartition(rddOfDataset, query)

        success = true
      } else if (serviceUri == "rdd:perGraph") {
        // Get the RDD[Dataset] from the execution context
        val rddOfDataset: RDD[Dataset] = execCxt.getContext.get(SYM_RDD_OF_DATASET)

        if (rddOfDataset == null) {
          throw new RuntimeException("No rddOfDataset in execution context - cannot delegate to " + serviceUri)
        }

        val query = if (op.getServiceElement != null) {
          QueryUtils.elementToQuery(op.getServiceElement)
        } else {
          OpAsQuery.asQuery(op.getSubOp)
        }

        result = RddOfDatasetOps.flatMapWithSparqlSelect(rddOfDataset, query)

        success = true
      }
    }

    if (!success) {
      throw new IllegalArgumentException("Execution with service " + serviceNode + " is not supportd")
    }

    result
  }

  override def execute(op: OpOrder, rdd: RDD[Binding]): RDD[Binding] =
    RddOfBindingOps.order(exec(op.getSubOp, rdd), op.getConditions)

  override def execute(op: OpExtend, rdd: RDD[Binding]): RDD[Binding] =
    RddOfBindingOps.extend(exec(op.getSubOp, rdd), op.getVarExprList)

  override def execute(op: OpUnion, rdd: RDD[Binding]): RDD[Binding] = {
    // TODO This method should get the (spark-based) executor
    // and pass all union members to it
    ???
  }


  override def execute(op: OpDistinct, rdd: RDD[Binding]): RDD[Binding] =
    exec(op.getSubOp, rdd).distinct()

  override def execute(op: OpReduced, rdd: RDD[Binding]): RDD[Binding] =
    exec(op.getSubOp, rdd).distinct()

  override def execute(op: OpFilter, rdd: RDD[Binding]): RDD[Binding] = {
    RddOfBindingOps.filter(exec(op.getSubOp, rdd), op.getExprs)
    // RddOfBindingOps.filter(rdd, op.getExprs)
  }

  //  override def execute(op: OpSlice, rdd: RDD[Binding]): RDD[Binding] =
  override def execute(op: OpSlice, rdd: RDD[Binding]): RDD[Binding] = ???
}
