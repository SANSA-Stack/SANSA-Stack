package net.sansa_stack.query.spark.rdd.op

import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.api.impl.ResultSetSparkImpl
import net.sansa_stack.query.spark.engine.{ExecutionDispatch, OpExecutorImpl}
import org.aksw.commons.collector.core.AggInputBroadcastMap.AccInputBroadcastMap
import org.aksw.commons.collector.core.{AggBuilder, AggInputBroadcastMap}
import org.aksw.commons.collector.domain.ParallelAggregator
import org.aksw.commons.lambda.serializable.SerializableSupplier
import org.aksw.jenax.arq.analytics.arq.ConvertArqAggregator
import org.aksw.jenax.arq.util.binding.BindingUtils
import org.aksw.jenax.arq.util.exec.query.ExecutionContextUtils
import org.aksw.jenax.arq.util.syntax.VarExprListUtils
import org.apache.jena.graph.Node
import org.apache.jena.query.{ARQ, Dataset, Query, SortCondition}
import org.apache.jena.sparql.ARQConstants
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.core.{Var, VarExprList}
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.binding.{Binding, BindingBuilder, BindingComparator, BindingFactory}
import org.apache.jena.sparql.expr.{Expr, ExprAggregator, ExprList}
import org.apache.jena.sparql.function.FunctionEnv
import org.apache.jena.sparql.util.{Context, NodeFactoryExtra}
import org.apache.spark.rdd.RDD

import java.util
import scala.reflect.classTag

/** Operations on RDD[Binding] */
object RddOfBindingsOps {
  //  implicit val NodeValueOrdering = new Ordering[NodeValue] {
  //    override def compare(a: NodeValue, b: NodeValue) = NodeValue.compareAlways(a, b)
  //  }


  // FIXME This would actually belong to RddOfDatasetOps (in sansa-jena-spark)
  // however our query api for spark (e.g. ResultSetSpark) is part of the query module
  def execSparqlSelect(rddOfDataset: RDD[_ <: Dataset], query: Query, cxtSupplier: SerializableSupplier[Context]): ResultSetSpark = {
    val op = Algebra.compile(query)

    // Set up an execution context
    // TODO ... allow passing that as a parameter
    // val cxt = if (cxt == null) ARQ.getContext.copy() else cxt.copy
    val execCxtSupplier = () => {
      val cxt = ARQ.getContext.copy()
      cxt.set(ARQConstants.sysCurrentTime, NodeFactoryExtra.nowAsDateTime)
      val execCxt = new ExecutionContext(cxt, null, null, null)
      execCxt.getContext.put(OpExecutorImpl.SYM_RDD_OF_DATASET, rddOfDataset)
      execCxt
    }

    val opExec = new OpExecutorImpl(() => execCxtSupplier.apply())
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
    // BindingProject becomes extremely slow when only few variables are selected from many
    // rddOfBindings.mapPartitions(_.map(new BindingProject(varList, _)))
    rddOfBindings.mapPartitions(it => it.map(BindingUtils.project(_, varList)))
  }

  // filterKeep
  def filter(rdd: RDD[Binding], expr: Expr): RDD[Binding] = {
    rdd.filter(binding => expr.eval(binding, null).getBoolean)
  }

  def filter(rdd: RDD[Binding], exprs: ExprList): RDD[Binding] = {
    // 'Hack' to inject the ExprList into the filter operation
    // val serializableExpr = E_SerializableIdentity.wrap(new E_Coalesce(exprs))
    val broadcast = rdd.context.broadcast(exprs)
    rdd.mapPartitions(it => {
      val el = broadcast.value
      val execCxt = ExecutionContextUtils.createExecCxtEmptyDsg()
      it.filter(el.isSatisfied(_, execCxt))
    })
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
    val subAggMap = new util.LinkedHashMap[Var, ParallelAggregator[Binding, Void, Node, _]]

    // I gave up on trying to get this working with fluent style + lambdas
    // due to compiler (tough NOT THE IDE) complaining about type errors; here's a working for loop instead:
    for (exprAgg <- aggregators.asScala) {
      val pagg: ParallelAggregator[Binding, Void, Node, _] = ConvertArqAggregator.convert(exprAgg.getAggregator)
      subAggMap.put(exprAgg.getVar, pagg)
    }

    //      aggregators.stream.collect(Collectors.toMap(
    //        (x: ExprAggregator) => x.getVar,
    //        (x: ExprAggregator) => ConvertArqAggregator.convert(x.getAggregator)))

    //    val subAggMap: util.Map[Var, ParallelAggregator[Binding, Node, _]] =
    //      aggregators.asScala.map(x => (x.getVar, ConvertArqAggregator.convert(x.getAggregator)))
    //        .toMap.asJava

    val agg: AggInputBroadcastMap[Binding, Void, Var, Node] = AggBuilder.inputBroadcastMap(subAggMap)
    val groupVarsBc = rdd.context.broadcast(groupVars)
    val aggBc = rdd.context.broadcast(agg)

    val result: RDD[Binding] = rdd
      .mapPartitions(it => {
        val agg = aggBc.value
        val groupKeyToAcc = new util.LinkedHashMap[Binding, AccInputBroadcastMap[Binding, Void, Var, Node]]()

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
        val r: BindingBuilder = BindingFactory.builder
        r.addAll(keyAndMap._1)

        val map: util.Map[Var, Node] = keyAndMap._2.getValue
        map.forEach((v, n) => r.add(v, n))

        r.build()
      }))

    result
  }

  /*
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
  */

  /**
   * Sort an RDD w.r.t. a given list of [[SortCondition]]s.
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
      val broadcast = rdd.context.broadcast(sortConditions)
      val projectVars = new util.HashSet[Var]
      sortConditions.forEach(x => projectVars.addAll(x.getExpression.getVarsMentioned))
      // val bindingToKey = (b: Binding) => new BindingProject(projectVars, b).asInstanceOf[Binding]
      val bindingToKey = (b: Binding) => BindingUtils.project(b, projectVars).asInstanceOf[Binding]

      // XXX What is better - Binding or Tuple? The latter makes for smaller sort keys but requires mapping to Binding again.
      // var projVars = exprs.getVarsMentioned.toArray(new Array[Var](0))
      // val bindingToKey = (b: Binding) => BindingUtils.projectAsTuple(b, projVars)

      val order = new Ordering[Binding] {
        def bindingComparator = new BindingComparator(broadcast.value)

        override def compare(x: Binding, y: Binding): Int = bindingComparator.compare(x, y)
      }

      // sortBy immediately triggers parsing the input rdd; for this reason we may want to use cache/persist.
      // However, it may be better not make this choice in the planner rather than the executor
      // rdd.persist(StorageLevel.MEMORY_AND_DISK)
      rdd.sortBy(bindingToKey, true)(order, classTag[Binding])
    }
  }

  /** This method does not result in a sorted RDD. Bindings are compared by means of evaluation of the given conditions. */
  def distinctByConditions(rdd: RDD[Binding], sortConditions: util.List[SortCondition]): RDD[Binding] = {
    val broadcast = rdd.context.broadcast(sortConditions)
    val order = new Ordering[Binding] {
      def bindingComparator = new BindingComparator(broadcast.value)

      override def compare(x: Binding, y: Binding): Int = bindingComparator.compare(x, y)
    }
    // XXX What is a good value for numPartitions?
    rdd.distinct(rdd.getNumPartitions)(order)
  }

  def extend(rdd: RDD[Binding], varExprList: VarExprList, execCxtSupplier: () => ExecutionContext): RDD[Binding] = {
    val velBc = rdd.context.broadcast(varExprList)
    rdd.mapPartitions(it => {
      val execCxt = execCxtSupplier.apply()
      val vel = velBc.value
      it.map(b => {
        val r = VarExprListUtils.eval(vel, b, execCxt)
        r
      })
    })
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
