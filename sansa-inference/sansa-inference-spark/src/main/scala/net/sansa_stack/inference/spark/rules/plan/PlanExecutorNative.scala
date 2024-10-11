package net.sansa_stack.inference.spark.rules.plan

import java.util

import scala.jdk.CollectionConverters._
import scala.collection.mutable

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, EqualTo, Expression, IsNotNull, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.{Inner, logical}

import net.sansa_stack.inference.data.Jena
import net.sansa_stack.inference.spark.data.model.{EmptyRDFGraphDataFrame, RDFGraphNative}
import net.sansa_stack.inference.utils.{Logging, Tuple0}

/**
  * An executor that works on the the native Scala data structures and uses Spark joins, filters etc.
  *
  * @author Lorenz Buehmann
  */
class PlanExecutorNative(sc: SparkContext)
  extends PlanExecutor[Jena, RDD[Triple], Node, Triple, RDFGraphNative]
    with Logging{

  private val sqlContext = SparkSession.builder().getOrCreate().sqlContext
  private val emptyGraph = EmptyRDFGraphDataFrame.get(sqlContext)

  private val maxFields = Integer.MAX_VALUE

  def execute(plan: Plan, graph: RDFGraphNative): RDFGraphNative = {
    // generate logical execution plan
    val logicalPlan = plan.toLogicalPlan(sqlContext)
    debug(logicalPlan.toString())

    // execute the plan
    val result = executePlan(logicalPlan, graph.toRDD().asInstanceOf[RDD[Product]])
    trace("RESULT:\n" + result.collect().mkString("\n"))

    // map to RDF triples
    val newGraph = new RDFGraphNative(
      result.map(t =>
        Triple.create(
          t.productElement(0).asInstanceOf[Node],
          t.productElement(1).asInstanceOf[Node],
          t.productElement(2).asInstanceOf[Node]))
    )

    // return new graph
    newGraph
  }

  def performJoin(leftRDD: RDD[Product], rightRDD: RDD[Product],
                  leftExpressions: List[Expression], rightExpressions: List[Expression],
                  joinCondition: Expression): RDD[Product] = {
    debug("JOIN ON " + joinCondition)
    trace("L:\n" + leftRDD.collect().mkString("\n"))
    trace("R:\n" + rightRDD.collect().mkString("\n"))

    trace("EXPR L:" + leftExpressions)
    trace("EXPR R:" + rightExpressions)

    // get expressions used in join conditions
    val joinExpressions = expressionsFor(joinCondition, isJoin = true)
    trace("JOIN EXPR:" + joinExpressions)

    // get left and right join expressions
    val joinExpressionsLeft = joinExpressions.filter(expr =>
      leftExpressions.exists(leftExpr => leftExpr.semanticEquals(expr)))
    val joinExpressionsRight = joinExpressions.filter(expr =>
      rightExpressions.exists(rightExpr => rightExpr.semanticEquals(expr)))
    trace("JOIN EXPR L:" + joinExpressionsLeft)
    trace("JOIN EXPR R:" + joinExpressionsRight)

    // get positions of expressions
    val joinPositionsLeft = joinExpressionsLeft.map(expr => {
      val leftExpr = leftExpressions.find(_ semanticEquals expr).get
      leftExpressions.indexOf(leftExpr)
    })
    val joinPositionsRight = joinExpressionsRight.map(expr => {
      val rightExpr = rightExpressions.find(_ semanticEquals expr).get
      rightExpressions.indexOf(rightExpr)
    })
    trace("JOIN POS L:" + joinPositionsLeft)
    trace("JOIN POS R:" + joinPositionsRight)

    // convert to PairRDDs
    val l = toPairRDD(leftRDD, joinPositionsLeft)
    val r = toPairRDD(rightRDD, joinPositionsRight)
    trace("L PAIR:\n" + l.collect().mkString("\n"))
    trace("R PAIR:\n" + r.collect().mkString("\n"))

    // perform join
    val joinedRDD = l.join(r)
    trace("JOINED\n" + joinedRDD.collect().mkString("\n"))

    // map it back to tuples
    val result = mergedRDD(joinedRDD, joinPositionsLeft)
    trace("MERGED\n" + result.collect().mkString("\n"))

    result
  }

  def performProjection(rdd: RDD[Product], projectList: Seq[Expression],
                        childExpressions: Seq[Expression], joinConditions: Seq[EqualTo]): RDD[Product] = {
    debug("PROJECTION")
    trace("PROJECTION VARS:" + projectList)
    trace("CHILD EXPR:" + childExpressions)

    var result = rdd

    // in case of Join, we rewrite the projection list
    var projectListNew = projectList.map(expr => {
      if (joinConditions.exists(cond => cond.right.simpleString(maxFields) == expr.simpleString(maxFields))) joinConditions.head
      else expr
    }
    )


    val availableExpressionsReal = childExpressions.distinct
    trace("CHILD EXPR(REAL):" + availableExpressionsReal)

    // get aliase
    var aliases = mutable.Seq[(Int, Expression)]()
    aliases ++= projectList
      .filter(expr => expr.isInstanceOf[Alias])
      .map(expr => projectList.indexOf(expr) -> expr.asInstanceOf[Alias].child)

    // if size of projection or ordering is different, or there is an alias var
    if(projectList.size != childExpressions.size ||
      !projectList.equals(childExpressions) ||
      aliases.nonEmpty) {

      // get the positions for projection
      val positions = projectList.map(expr => availableExpressionsReal.indexOf(expr))
      trace("EXTR POSITIONS:" + positions)

      result = rdd map genMapper(tuple => extract(tuple, positions, aliases))
      trace("MAPPING RESULT\n" + result.collect().mkString("\n"))
    }
    result
  }

  def performProjection(rdd: RDD[Product], projectList: Seq[NamedExpression], child: LogicalPlan): RDD[Product] = {
    debug("PROJECTION")
    trace(projectList.map(expr => expr.simpleString(maxFields)).mkString(","))

    var resultRDD = rdd

    var projectionVars: Seq[Expression] = projectList

    var joinConditions = Seq[EqualTo]()

    // get the available child expressions
    val childExpressions = (child match {
      case logical.Filter(condition, filterChild) => expressionsFor(filterChild)
      case logical.Join(left, right, Inner, Some(condition), joinHint) =>
        var list = new mutable.ListBuffer[Expression]()
        list ++= expressionsFor(left) ++ expressionsFor(right)
        val eCond = expressionsFor(condition, isJoin = true).map(expr => expr.simpleString(maxFields))
        val eRight = expressionsFor(right)
        val joins = joinConditionsFor(condition)
        var list2 = new mutable.ListBuffer[Expression]()
        list.foreach{expr =>
          var replace: Option[Expression] = None
          joins.foreach{j =>
            if (j.right.simpleString(maxFields) == expr.simpleString(maxFields)) {
              replace = Some(j.left)
            }
          }
          if(replace.isDefined) {
            list2 += replace.get
          } else {
            list2 += expr
          }
        }
        for(e <- eRight) {
          if (eCond.contains(e.simpleString(maxFields))) {
            list -= e
          }
        }

        //
        var projectList2 = new mutable.ListBuffer[Expression]()
        projectList.foreach{expr =>
          var replace: Option[Expression] = None
          joins.foreach{j =>
            if (j.right.simpleString(maxFields) == expr.simpleString(maxFields)) {
              replace = Some(j.left)
            }
          }
          if(replace.isDefined) {
            projectList2 += replace.get
          } else {
            projectList2 += expr
          }
        }
        projectionVars = projectList2

        list2.toList
        joinConditions = joinConditionsFor(condition)

        list
      case _ => expressionsFor(child)
    })
      .map(expr => expr.asInstanceOf[AttributeReference].simpleString(maxFields))

    trace("CHILD EXPR:" + childExpressions)
    trace("PROJECTION VARS:" + projectionVars)

    val availableExpressionsReal = childExpressions.distinct
    trace("CHILD EXPR(REAL):" + availableExpressionsReal)

    var aliases = mutable.Seq[(Int, Expression)]()
    aliases ++= projectionVars
      .filter(expr => expr.isInstanceOf[Alias])
      .map(expr => projectionVars.indexOf(expr) -> expr.asInstanceOf[Alias].child)
    trace("ALIASE:" + availableExpressionsReal)

    // if size of projection or ordering is different, or there is an alias var
    val projectionVarsStr = projectionVars.map(_.simpleString(maxFields))
    if(projectionVars.size != childExpressions.size ||
      !projectionVarsStr.equals(childExpressions) ||
      aliases.nonEmpty) {

      val positions = projectionVars.map(expr => availableExpressionsReal.indexOf(expr.simpleString(maxFields)))

      trace("EXTR POSITIONS:" + positions)

      resultRDD = rdd map genMapper(tuple => extract(tuple, positions, aliases))
      trace("MAPPING RESULT\n" + resultRDD.collect().mkString("\n"))
    }
    resultRDD
  }

  def executePlan[T >: Product, U <: Product](logicalPlan: LogicalPlan, triples: RDD[Product]): RDD[Product] = {
    logicalPlan match {
      case logical.Join(left, right, Inner, Some(condition), joinHint) =>
        // process left child
        val leftRDD = executePlan(left, triples)

        // process right child
        val rightRDD = executePlan(right, triples)

        // perform join
        performJoin(leftRDD, rightRDD, expressionsFor(left), expressionsFor(right), condition)
      case logical.Project(projectList, child) =>
        // process child
        var rdd = executePlan(child, triples)

        // perform projection
        rdd = performProjection(rdd, projectList, child)

        rdd
      case logical.Filter(condition, child) =>
        // process child
        val childRDD = executePlan(child, triples)

        // apply the filter
        val childExpressions = expressionsFor(child)
        applyFilter(condition, childExpressions, childRDD)
      case default =>
        trace(default.simpleString(maxFields))
        triples
    }
  }

  def joinConditionsFor(expr: Expression): List[EqualTo] = {
    expr match {
      case And(left: Expression, right: Expression) =>
        joinConditionsFor(left) ++ joinConditionsFor(right)
      case EqualTo(left: Expression, right: Expression) =>
        List(EqualTo(left: Expression, right: Expression))
      case _ =>
        Nil
    }
  }

  def extract[T <: Product](tuple: T, positions: Seq[Int], aliases: mutable.Seq[(Int, Expression)]): Product = {
    val list = tuple.productIterator.toList
    val mutList = mutable.ListBuffer[(Int, Expression)]()
    mutList ++= aliases
    val newList = positions.map(pos => if (pos == -1) mutList.remove(0)._2.toString() else list(pos))
    newList.toTuple
  }

  def genMapper[A, B](f: A => B): A => B = {
    val locker = com.twitter.chill.MeatLocker(f)
    x => locker.get.apply(x)
  }

  def asKeyValue(tuple: Product, keyPositions: Seq[Int]): (Product, Product) = {
//    println("TUPLE:" + tuple + "|POSITIONS:" + keyPositions)
    val key = keyPositions.map(pos => tuple.productElement(pos)).toTuple
    val value = for (i <- 0 until tuple.productArity; if !keyPositions.contains(i)) yield tuple.productElement(i)

    key -> value.toTuple
  }

  def toPairRDD[T >: Product](tuples: RDD[Product], joinPositions: Seq[Int]): RDD[(Product, Product)] = {
    tuples map genMapper(t => asKeyValue(t, joinPositions))
  }

  def mergeKeyValue(pair: (Product, (Product, Product)), joinPositions: Seq[Int]): Product = {
    val list = new util.LinkedList[Any]()
//    println("PAIR:" + pair)

    for(i <- 0 until pair._2._1.productArity) {
      list.add(pair._2._1.productElement(i))
    }

    for(i <- 0 until pair._2._2.productArity) {
      list.add(pair._2._2.productElement(i))
    }

    joinPositions.sorted.foreach(pos => list.add(pos, pair._1.productElement(joinPositions.indexOf(pos))))
//    for(i <- 0 until pair._1.productArity) {
//     list.add(joinPositions(i), pair._1.productElement(i))
//    }

    list.asScala.toList.toTuple
  }

  def mergedRDD(tuples: RDD[(Product, (Product, Product))], joinPositions: Seq[Int]): RDD[Product] = {
    trace("JOIN POS:" + joinPositions)
    tuples map genMapper(t => mergeKeyValue(t, joinPositions))
  }

  def applyFilter[T <: Product](condition: Expression, childExpressions: List[Expression], rdd: RDD[T]): RDD[T] = {

    condition match {
      case And(left: Expression, right: Expression) =>
        applyFilter(right, childExpressions, applyFilter(left, childExpressions, rdd))
      case EqualTo(left: Expression, right: Expression) =>
        debug("FILTER " + condition.simpleString(maxFields))
        val value = right.toString()

        val index = childExpressions.map(e => e.toString()).indexOf(left.toString())

        rdd.filter(t => t.productElement(index) == value)
      case IsNotNull(child: Expression) =>
        rdd
      case _ => rdd
    }
  }

  def expressionsFor(logicalPlan: LogicalPlan): List[Expression] = {
    logicalPlan match {
      case logical.Join(left, right, Inner, Some(condition), joinHint) =>
        expressionsFor(left) ++ expressionsFor(right)
      case logical.Project(projectList, child) =>
        projectList.toList
      case logical.Filter(condition, child) =>
        expressionsFor(child)
      case SubqueryAlias(alias: AliasIdentifier, child: LogicalPlan) =>
        expressionsFor(child)
      case _ =>
        logicalPlan.expressions.toList
    }
  }

  def expressionsFor(expr: Expression, isJoin: Boolean = false): List[Expression] = {
    expr match {
      case And(left: Expression, right: Expression) =>
        expressionsFor(left, isJoin) ++ expressionsFor(right, isJoin)
      case EqualTo(left: Expression, right: Expression) =>
        List(left) ++ (if (isJoin) List(right) else List())
      case IsNotNull(child: Expression) =>
        expressionsFor(child, isJoin)
      case _ =>
        Nil
    }
  }

  implicit class EnrichedWithToTuple[A](elements: Seq[A]) {
    def toTuple: Product = elements.length match {
      case 0 => Tuple0
      case 1 => toTuple1
      case 2 => toTuple2
      case 3 => toTuple3
      case 4 => toTuple4
      case 5 => toTuple5
      case 6 => toTuple6
      case 7 => toTuple7
      case 8 => toTuple8
      case 9 => toTuple9
      case 10 => toTuple10
    }
    def toTuple1: Product = elements match {case Seq(a) => Tuple1(a) }
    def toTuple2: Product = elements match {case Seq(a, b) => (a, b) }
    def toTuple3: Product = elements match {case Seq(a, b, c) => (a, b, c) }
    def toTuple4: Product = elements match {case Seq(a, b, c, d) => (a, b, c, d) }
    def toTuple5: Product = elements match {case Seq(a, b, c, d, e) => (a, b, c, d, e) }
    def toTuple6: Product = elements match {case Seq(a, b, c, d, e, f) => (a, b, c, d, e, f) }
    def toTuple7: Product = elements match {case Seq(a, b, c, d, e, f, g) => (a, b, c, d, e, f, g) }
    def toTuple8: Product = elements match {case Seq(a, b, c, d, e, f, g, h) => (a, b, c, d, e, f, g, h) }
    def toTuple9: Product = elements match {case Seq(a, b, c, d, e, f, g, h, i) => (a, b, c, d, e, f, g, h, i) }
    def toTuple10: Product = elements match {case Seq(a, b, c, d, e, f, g, h, i, j) => (a, b, c, d, e, f, g, h, i, j) }

  }
}
