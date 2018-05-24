package net.sansa_stack.query.spark.graph.jena.model

import net.sansa_stack.query.spark.graph.jena.resultOp.ResultGroup
import net.sansa_stack.query.spark.graph.jena.util.{BasicGraphPattern, Result, ResultFactory, ResultMapping}
import org.apache.jena.graph.Node
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.model.graph._
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.expr.ExprAggregator
import org.apache.jena.sparql.expr.aggregate.AggAvg
import org.apache.spark.rdd.RDD

/**
  * Model that contains methods for query operations on top of spark.
  */
object SparkExecutionModel {

  private var spark: SparkSession = _

  private var graph: Graph[Node, Node] = _

  def createSparkSession(): Unit = {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("s2x")
      .getOrCreate()
  }

  def loadGraph(path: String, lang: Lang): Unit = {
    if(spark == null){
      createSparkSession()
    }
    val triples = spark.rdf(lang)(path)
    graph = triples.asGraph().cache()
  }

  def basicGraphPatternMatch(bgp: BasicGraphPattern): RDD[Result[Node]] = {
    setSession()
    ResultFactory.create(ResultMapping.run(graph, bgp, spark), spark).cache()
  }

  def project(result: RDD[Result[Node]], varSet: Set[Node]): RDD[Result[Node]] = {
    val varBroadcast = spark.sparkContext.broadcast(varSet)
    val newResult = result.map(result => result.project(varBroadcast.value)).cache()
    result.unpersist()
    varBroadcast.unpersist()
    newResult
  }

  def distinct(result: RDD[Result[Node]]): RDD[Result[Node]] = {
    val newResult = result.distinct().cache()
    result.unpersist()
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
    aggregates.foreach{aggr =>
      println("getVar: "+aggr.getVar.asNode())
      println("getAggregator: "+aggr.getAggregator)
      println("aggr type: "+aggr.getAggregator.isInstanceOf[AggAvg])
      group.foreach{ case(_, iter) =>
      //ResultGroup.aggregateOp(iter, broadcast)
    }}
    result
  }

  def leftJoin(left: RDD[Result[Node]], right: RDD[Result[Node]]): RDD[Result[Node]] = {
    val leftVars = getVars(left)
    val rightVars = getVars(right)
    val intersection = spark.sparkContext.broadcast(leftVars.intersect(rightVars))
    var newResult: RDD[Result[Node]] = null
    // two results have no common variables
    if(intersection.value.isEmpty){
      newResult = left.cartesian(right).map{ case(r1, r2) => r1.merge(r2) }.cache()
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
