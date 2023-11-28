package net.sansa_stack.query.spark.graph.jena.model

import net.sansa_stack.query.spark.graph.jena.expression.{Expression, Filter, Pattern}
import net.sansa_stack.query.spark.graph.jena.resultOp.ResultGroup
import net.sansa_stack.query.spark.graph.jena.util._
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph.Node
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.expr.ExprAggregator
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Model that contains methods for query operations on top of spark.
 */
object SparkExecutionModel {

  private var spark: SparkSession = _

  private var graph: Graph[Node, Node] = _

  def setSparkSession(spark: SparkSession): Unit = {
    this.spark = spark
  }

  def createSparkSession(): Unit = {

    if (Config.getMaster == "") {
      Config.setMaster("local[*]")
    }

    if (Config.getInputGraphFile == "") {
      throw new ExceptionInInitializerError("Input graph file path is not initialized")
    }

    if (Config.getInputQueryFile == "") {
      throw new ExceptionInInitializerError("Input query file path is not initialized")
    }

    if (Config.getLang == null) {
      throw new ExceptionInInitializerError("The language of input graph file is not initialized")
    }

    spark = SparkSession.builder()
      .master(Config.getMaster)
      .appName(Config.getAppName)
      .getOrCreate()

    loadGraph()
  }

  def createSparkSession(session: SparkSession): Unit = {
    if (spark == null) {
      spark = session
    } else {
      throw new IllegalArgumentException("spark session has been set already")
    }
  }

  def loadGraph(): Unit = {
    loadGraph(Config.getInputGraphFile, Config.getLang)
  }

  def loadGraph(path: String, lang: Lang): Unit = {
    if (spark == null) {
      createSparkSession()
    }
    graph = spark.rdf(lang)(path).asGraph().cache()
  }

  def loadGraph(graph: Graph[Node, Node]): Unit = {
    this.graph = graph
  }

  def basicGraphPatternMatch(bgp: BasicGraphPattern): RDD[Result[Node]] = {

    if (spark == null) { setSession() }

    val patterns = spark.sparkContext.broadcast(bgp.triplePatterns)

    // Step 1: generate a new candidate graph, attribute of each vertex is a set of candidates match triple patterns
    val candidateGraph = MatchSet.createCandidateGraph(graph, patterns).cache()
    graph.unpersist()

    // Step 2: validate local match sets, filter candidate which has local match.
    val localGraph = MatchSet.localMatch(candidateGraph, patterns).cache()
    candidateGraph.unpersist()

    // Step 3: join the message of neighbours' candidates into vertex attributes
    val mergedGraph = MatchSet.joinNeighbourCandidate(localGraph).cache()
    localGraph.unpersist()

    // Step 4: validate remote match sets, filter candidate which has remote match.
    val remoteGraph = MatchSet.remoteMatch(mergedGraph).cache()
    mergedGraph.unpersist()

    // Step 5: Produce this final results that match the triple patterns in RDF graph.
    val results = MatchSet.generateResultRDD(remoteGraph, patterns, spark).cache()
    remoteGraph.unpersist()

    results
  }

  def project(result: RDD[Result[Node]], varSet: Set[Node]): RDD[Result[Node]] = {
    val varBroadcast = spark.sparkContext.broadcast(varSet)
    val newResult = result.map(result => result.project(varBroadcast.value)).cache()
    result.unpersist()
    varBroadcast.unpersist()
    newResult
  }

  def distinct(result: RDD[Result[Node]]): RDD[Result[Node]] = {
    /* val newResult = result.distinct().cache()
    result.unpersist() */
    val newResult = spark.sparkContext.parallelize(result.collect().distinct).cache()
    newResult
  }

  def slice(result: RDD[Result[Node]], limit: Int, offset: Int): RDD[Result[Node]] = {
    val newArray = result.collect().slice(offset, limit + offset)
    result.unpersist()
    val newResult = spark.sparkContext.parallelize(newArray).cache()
    newResult
  }

  def group(result: RDD[Result[Node]], vars: List[Node], aggregates: List[ExprAggregator]): RDD[Result[Node]] = {
    val group = result.groupBy(r => r.projectNewResult(vars.toSet)).cache()
    var newResult: RDD[Result[Node]] = group.map(_._1)
    result.unpersist()
    aggregates.foreach { aggr =>
      val variable = aggr.getVar
      val aggrOp = aggr.getAggregator.getName
      val key = aggr.getAggregator.getExprList.get(0).asVar
      newResult = this.leftJoin(
        newResult,
        group.map(pair => ResultFactory.merge(pair._1, ResultGroup.aggregateOp(pair._2, variable, aggrOp, key))))
    }
    group.unpersist()
    newResult
  }

  def extend(result: RDD[Result[Node]], sub: Node, expr: Node): RDD[Result[Node]] = {
    val newResult = result.map(r => r.addMapping(sub, r.getValue(expr))).cache()
    result.unpersist()
    newResult
  }

  def filter(result: RDD[Result[Node]], filters: List[Expression]): RDD[Result[Node]] = {
    val broadcast = spark.sparkContext.broadcast(filters)
    var intermediate: RDD[Result[Node]] = result.cache()
    broadcast.value.foreach {
      case e: Filter => intermediate = intermediate.filter(r => e.evaluate(r))
      case e: Pattern => intermediate = e.evaluate(intermediate)
    }
    intermediate
  }

  def leftJoin(left: RDD[Result[Node]], right: RDD[Result[Node]]): RDD[Result[Node]] = {
    val leftVars = getVars(left)
    val rightVars = getVars(right)
    val intersection = spark.sparkContext.broadcast(leftVars.intersect(rightVars))
    var newResult: RDD[Result[Node]] = null
    // two results have no common variables
    if (intersection.value.isEmpty) {
      newResult = left.cartesian(right).map { case (r1, r2) => r1.merge(r2) }.cache()
      left.unpersist()
      right.unpersist()
    } else {
      val leftPair = left.map(r => (r.getValueSet(intersection.value), r))
      left.unpersist()
      val rightPair = right.map(r => (r.getValueSet(intersection.value), r))
      right.unpersist()
      intersection.unpersist()

      newResult = leftPair.leftOuterJoin(rightPair).map {
        case (_, pair) =>
          pair._2 match {
            case Some(_) => pair._1.merge(pair._2.get)
            case None => pair._1
          }
      }.cache()
    }
    newResult
  }

  def union(left: RDD[Result[Node]], right: RDD[Result[Node]]): RDD[Result[Node]] = {
    val newResult = left.union(right).cache()
    left.unpersist()
    right.unpersist()
    newResult
  }

  def minus(left: RDD[Result[Node]], right: RDD[Result[Node]]): RDD[Result[Node]] = {
    val leftVars = getVars(left)
    val rightVars = getVars(right)
    val intersection = spark.sparkContext.broadcast(leftVars.intersect(rightVars))
    var newResult: RDD[Result[Node]] = null
    // two results have no common variables
    if (intersection.value.isEmpty) {
      newResult = left
    } else {
      val leftPair = left.map(result => (result.projectNewResult(intersection.value), result))
      val rightPair = right.map(result => (result, null))
      newResult = leftPair.subtractByKey(rightPair).map(pair => pair._2).cache()
    }
    left.unpersist()
    right.unpersist()
    newResult
  }

  def getSession: SparkSession = {
    spark
  }

  def getGraph: Graph[Node, Node] = {
    graph
  }

  private def setSession(): Unit = {
    if (spark == null) {
      createSparkSession()
    }
  }

  private def getVars(results: RDD[Result[Node]]): Set[Node] = {
    results.map(result => result.getField).reduce((s1, s2) => s1 ++ s2)
  }
}
