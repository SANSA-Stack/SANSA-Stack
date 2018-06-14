package net.sansa_stack.query.spark.graph.jena.model

import net.sansa_stack.query.spark.graph.jena.expression.{Expression, Filter, Pattern}
import net.sansa_stack.query.spark.graph.jena.resultOp.ResultGroup
import net.sansa_stack.query.spark.graph.jena.util._
import org.apache.jena.graph.Node
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.model.graph._
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.expr.ExprAggregator
import org.apache.spark.rdd.RDD

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

    if(Config.getMaster == ""){
      Config.setMaster("local[*]")
    }

    if(Config.getInputGraphFile == ""){
      throw new ExceptionInInitializerError("Input graph file path is not initialized")
    }

    if(Config.getInputQueryFile == ""){
      throw new ExceptionInInitializerError("Input query file path is not initialized")
    }

    if(Config.getLang == null){
      throw new ExceptionInInitializerError("The language of input graph file is not initialized")
    }

    spark = SparkSession.builder()
      .master(Config.getMaster)
      .appName(Config.getAppName)
      // Use the Kryo serializer
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "net.sansa_stack.query.spark.graph.jena.serialization.Registrator")
      //.config("spark.core.connection.ack.wait.timeout", "5000")
      //.config("spark.shuffle.consolidateFiles", "true")
      //.config("spark.rdd.compress", "true")
      //.config("spark.kryoserializer.buffer.max.mb", "512")
      .getOrCreate()

    loadGraph()
  }

  def loadGraph(): Unit = {
    loadGraph(Config.getInputGraphFile, Config.getLang)
  }

  def loadGraph(path: String, lang: Lang): Unit = {
    if(spark == null){
      createSparkSession()
    }
    graph = spark.rdf(lang)(path).asGraph().cache()
  }

  def basicGraphPatternMatch(bgp: BasicGraphPattern): RDD[Result[Node]] = {

    if(spark == null){ setSession() }

    val patterns = spark.sparkContext.broadcast(bgp.triplePatterns)
    val matchSet = new MatchSet(graph, patterns, spark)
    var finalMatchSet = matchSet.matchCandidateSet.cache()
    var prevMatchSet: RDD[MatchCandidate] = null

    var changed = true
    while(changed) {
      prevMatchSet = finalMatchSet
      // Step 1: Confirm the validation of local match sets.
      prevMatchSet = matchSet.validateLocalMatchSet(prevMatchSet)

      // Step 2: Conform the validation of remote match sets.
      prevMatchSet = matchSet.validateRemoteMatchSet(prevMatchSet)

      if(prevMatchSet.count().equals(finalMatchSet.count())){
        changed = false
      }
      finalMatchSet = prevMatchSet
      prevMatchSet.unpersist()
    }

    if(finalMatchSet.count()==0){
      throw new IllegalStateException("No results match")
    }

    var intermediate: RDD[Result[Node]] = null
    patterns.value.foreach{ pattern =>
      val mapping = finalMatchSet
        .filter(_.pattern.equals(pattern))
        .map(_.mapping).collect()
        .map(_.filterKeys(_.toString.startsWith("?")))
        .distinct
      if(intermediate == null) {
        intermediate = ResultFactory.create(mapping, spark).cache()
      }
      else{
        intermediate = leftJoin(intermediate, ResultFactory.create(mapping, spark)).cache()
      }
    }
    finalMatchSet.unpersist()
    graph.unpersist()

    intermediate
  }

  def project(result: RDD[Result[Node]], varSet: Set[Node]): RDD[Result[Node]] = {
    val varBroadcast = spark.sparkContext.broadcast(varSet)
    val newResult = result.map(result => result.project(varBroadcast.value)).cache()
    result.unpersist()
    varBroadcast.unpersist()
    newResult
  }

  def distinct(result: RDD[Result[Node]]): RDD[Result[Node]] = {
    /*val newResult = result.distinct().cache()
    result.unpersist()*/
    val newResult = spark.sparkContext.parallelize(result.collect().distinct).cache()
    newResult
  }

  def slice(result: RDD[Result[Node]], limit: Int, offset: Int): RDD[Result[Node]] = {
    val newArray = result.collect().slice(offset, limit+offset)
    result.unpersist()
    val newResult = spark.sparkContext.parallelize(newArray).cache()
    newResult
  }

  def group(result: RDD[Result[Node]], vars: List[Node], aggregates: List[ExprAggregator]): RDD[Result[Node]] = {
    val group = result.groupBy(r => r.projectNewResult(vars.toSet)).cache()
    var newResult: RDD[Result[Node]] = group.map(_._1)
    result.unpersist()
    aggregates.foreach{aggr =>
      val variable = aggr.getVar.asNode()
      val aggrOp = aggr.getAggregator.getName
      val key = aggr.getAggregator.getExprList.get(0).asVar.asNode
      newResult = this.leftJoin(newResult,
        group.map( pair => ResultFactory.merge(pair._1,ResultGroup.aggregateOp(pair._2, variable, aggrOp, key))))
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
    var intermediate:RDD[Result[Node]] = result.cache()
    broadcast.value.foreach{
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
    if(intersection.value.isEmpty){
      newResult = left.cartesian(right).map{ case(r1, r2) => r1.merge(r2) }.cache()
      left.unpersist()
      right.unpersist()
    }
    else {
      val leftPair = left.map(r => (r.getValueSet(intersection.value), r))
      left.unpersist()
      val rightPair = right.map(r => (r.getValueSet(intersection.value), r))
      right.unpersist()
      intersection.unpersist()

      newResult = leftPair.leftOuterJoin(rightPair).map{case(_, pair) =>
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
    if(intersection.value.isEmpty){
      newResult = left
    }
    else {
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
    if(spark == null){
      createSparkSession()
    }
  }

  private def getVars(results: RDD[Result[Node]]): Set[Node] = {
    results.map(result => result.getField).reduce((s1, s2) => s1++s2)
  }
}
