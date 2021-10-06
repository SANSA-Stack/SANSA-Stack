package net.sansa_stack.query.spark.rdd.op

import java.util

import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.api.impl.ResultSetSparkImpl
import net.sansa_stack.query.spark.binding.engine.{ExecutionDispatch, OpExecutorImpl}
import net.sansa_stack.rdf.spark.rdd.op.RddOfDatasetsOps
import org.aksw.commons.collector.core.AggInputBroadcastMap.AccInputBroadcastMap
import org.aksw.commons.collector.core.{AggBuilder, AggInputBroadcastMap}
import org.aksw.commons.collector.domain.ParallelAggregator
import org.aksw.jena_sparql_api.analytics.arq.ConvertArqAggregator
import org.aksw.jena_sparql_api.utils.{BindingUtils, QueryUtils, VarExprListUtils}
import org.apache.jena.graph.Node
import org.apache.jena.query.{ARQ, Dataset, Query, SortCondition}
import org.apache.jena.sparql.ARQConstants
import org.apache.jena.sparql.algebra.op.OpService
import org.apache.jena.sparql.algebra.{Algebra, OpAsQuery}
import org.apache.jena.sparql.core.{Var, VarExprList}
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.binding.{Binding, BindingFactory, BindingMap}
import org.apache.jena.sparql.expr.{Expr, ExprAggregator, ExprList, NodeValue}
import org.apache.jena.sparql.function.FunctionEnv
import org.apache.jena.sparql.util.NodeFactoryExtra
import org.apache.spark.rdd.RDD

import scala.reflect.classTag


/** Operations on RDD[Binding] */
object RddOfBindingsOps {
  //  implicit val NodeValueOrdering = new Ordering[NodeValue] {
  //    override def compare(a: NodeValue, b: NodeValue) = NodeValue.compareAlways(a, b)
  //  }


  // FIXME This belongs to RddOfDatasetOps
  def execSparqlSelect(rddOfDataset: RDD[_ <: Dataset], query: Query): ResultSetSpark = {
    val op = Algebra.compile(query)

    // Set up an execution context
    // TODO ... allow passing that as a parameter
    val cxt = ARQ.getContext.copy()
    cxt.set(ARQConstants.sysCurrentTime, NodeFactoryExtra.nowAsDateTime)
    val execCxt = new ExecutionContext(cxt, null, null, null)
    execCxt.getContext.put(OpExecutorImpl.SYM_RDD_OF_DATASET, rddOfDataset)
    val opExec = new OpExecutorImpl(execCxt)
    val executionDispatch = new ExecutionDispatch(opExec)

    // An RDD with a single binding that doesn't bind any variables
    val initialRdd = rddOfDataset.context.parallelize(Seq[Binding](BindingFactory.binding))
    val rdd = executionDispatch.exec(op, initialRdd)

    import collection.JavaConverters._
    val vars = query.getProjectVars.asScala.toList
    new ResultSetSparkImpl(vars, rdd)
  }


  /**
   * Return a new RDD[Binding] by projecting only the given variables
   *
   * @param rddOfBindings The input RDD of bindings
   * @param projectVars   The variables which to project
   * @return The RDD of bindings with the variables projected
   */
  def project(rddOfBindings: RDD[_ <: Binding], projectVars: util.Collection[Var]): RDD[Binding] = {
    val varList = projectVars
    rddOfBindings.mapPartitions(_.map(BindingUtils.project(_, varList)))
  }

  // filterKeep
  def filter(rdd: RDD[Binding], expr: Expr): RDD[Binding] = {
    rdd.filter(binding => expr.eval(binding, null).getBoolean)
  }

  def filter(rdd: RDD[Binding], exprs: ExprList): RDD[Binding] = {
    rdd.filter(binding => exprs.getList.stream().allMatch(_.eval(binding, null).getBoolean))
  }


  def naturalJoin(rdd1: RDD[Binding], rdd2: RDD[Binding], commonVars: util.Set[Var]): RDD[Binding] = {
    // This op might better fit into ResultSetSparkOps
    ???
  }

  def group(rdd: RDD[Binding], groupVars: VarExprList, aggregators: util.List[ExprAggregator]): RDD[Binding] = {
    import collection.JavaConverters._


//    val list = new util.ArrayList[Foo[_ >: AnyRef]]
//    val map: util.Map[Any, Foo[_]] = list.stream.collect(Collectors.toMap(y => y.value, y => y))


    // For each ExprVar convert the involvd arq aggregator
    val subAggMap = new util.LinkedHashMap[Var, ParallelAggregator[Binding, Node, _]]

    // I gave up on trying to get this working with fluent style + lambdas
    // due to compiler (tough NOT THE IDE) complaining about type errors; here's a working for loop instead:
    for (exprAgg <- aggregators.asScala) {
      val pagg = ConvertArqAggregator.convert(exprAgg.getAggregator)
      subAggMap.put(exprAgg.getVar, pagg)
    }

//      aggregators.stream.collect(Collectors.toMap(
//        (x: ExprAggregator) => x.getVar,
//        (x: ExprAggregator) => ConvertArqAggregator.convert(x.getAggregator)))

//    val subAggMap: util.Map[Var, ParallelAggregator[Binding, Node, _]] =
//      aggregators.asScala.map(x => (x.getVar, ConvertArqAggregator.convert(x.getAggregator)))
//        .toMap.asJava

    val agg: AggInputBroadcastMap[Binding, Var, Node] = AggBuilder.inputBroadcastMap(subAggMap)
    val groupVarsBc = rdd.context.broadcast(groupVars)
    val aggBc = rdd.context.broadcast(agg)

    val result: RDD[Binding] = rdd
      .mapPartitions(it => {
        val agg = aggBc.value
        val groupKeyToAcc = new util.LinkedHashMap[Binding, AccInputBroadcastMap[Binding, Var, Node]]()

        for (binding <- it) {
          val groupKey: Binding = VarExprListUtils.copyProject(groupVarsBc.value, binding, null)
          val acc = groupKeyToAcc.computeIfAbsent(groupKey, k => aggBc.value.createAccumulator())

          acc.accumulate(binding)
        }

        val r = groupKeyToAcc.entrySet.asScala.iterator.map(x => (x.getKey, x.getValue))
        r
      })
      // Combine accumulators for each group key
      .reduceByKey((a, b) => {
        val agg = aggBc.value
        val r = agg.combine(a, b)
        r
      })
      // Restore bindings from groupKey (already a binding)
      // and the accumulated values (instances of Map[Var, Node])
      .mapPartitions(_.map(keyAndMap => {
        val r: BindingMap = BindingFactory.create()
        r.addAll(keyAndMap._1)

        val map: util.Map[Var, Node] = keyAndMap._2.getValue
        map.forEach((v, n) => r.add(v, n))

        r.asInstanceOf[Binding]
      }))

    result
  }

  def serviceSpecial(rdd: RDD[Dataset], op: OpService): RDD[Binding] = {
    // Get the element / or op, create a SPARQL select query from it and
    // run it on the RDD of datasets
    val query = if (op.getServiceElement != null) {
      OpAsQuery.asQuery(op.getSubOp)
    } else {
      QueryUtils.elementToQuery(op.getServiceElement)
    }

    // RddOfDatasetOps.selectWithSparql(rdd, query)
    RddOfDatasetsOps.mapPartitionsWithSparql(rdd, query)
  }




  /**
   * Limitation: Only the first sort condition's sort direction is considered.
   * Spark's rdd.sortBy does not directly support multiple sort orders
   * Maybe it is possible to work around that in the future by creating custom key objects
   * with custom compareTo implementations
   *
   * @param rdd
   * @param sortConditions
   * @return
   */
  def order(rdd: RDD[Binding], sortConditions: util.List[SortCondition]): RDD[Binding] = {

    val env: FunctionEnv = null
    val n = sortConditions.size()

    if (n == 0) {
      // nothing to be done - error?
      rdd
    } else {
      val firstSortCondition = sortConditions.get(0)

      // By default sort by ASC
      val isAscending = firstSortCondition.getDirection != Query.ORDER_DESCENDING

      // FIXME Sort key... Comparable... Serializable... Ugh!
      val expr = firstSortCondition.getExpression
      val broadcastExpr = rdd.context.broadcast(expr)
      val bindingToKey = (b: Binding) => broadcastExpr.value.eval(b, env)

//      val bindingToKey: Binding => NodeValue =
//        if (n == 1) {
//          val expr = firstSortCondition.getExpression
//          (b: Binding) => expr.eval(b, env)
//        } else {
//          (b: Binding) => sortConditions.stream.map(_.getExpression).map(_.eval(b, env)).collect(Collectors.toList)
//        }

      // It seems we could here create our own comparator that implements
      // the different sort directions on the components
      implicit val order = new Ordering[NodeValue] {
        override def compare(x: NodeValue, y: NodeValue): Int = NodeValue.compareAlways(x, y)
      }

      rdd.sortBy(bindingToKey, isAscending)(order, classTag[NodeValue])
    }
  }

  def extend(rdd: RDD[Binding], varExprList: VarExprList): RDD[Binding] = {
    // TODO We should pass an execCxt
    val execCxt = null

    var broadcastVel = rdd.context.broadcast(varExprList)
    rdd.mapPartitions(_.map(b => {
      val r: BindingMap = BindingFactory.create()
      val contrib = VarExprListUtils.copyProject(broadcastVel.value, b, execCxt)
      r.addAll(b)
      r.addAll(contrib)
      r
    }))
  }


  /**
   * Collect all used datatype IRIs for each variable
   * mentioned in a RDD[Binding]
   *
   * IRI -> r2rml:IRI
   * BlankNode -> r2rml:BlankNode
   *
   * @return
   */
//  def usedDatatypeIris(): mutable.Map[Var, Multiset[String]] = {
//    null
//  }

  /**
   *
   * @return
   */
//  def usedIriPrefixes(rddOfBindings: RDD[_ <: Binding], bucketSize: Int = 1000): mutable.MultiMap[Var, RDFDatatype] = {
//    null
//  }


}
