package net.sansa_stack.inference.spark.backwardchaining

import java.io.PrintWriter

import org.apache.calcite.interpreter.Bindables.{BindableFilter, BindableJoin, BindableProject}
import org.apache.calcite.rel.{RelNode, RelVisitor}

import net.sansa_stack.inference.rules.RuleSets
import net.sansa_stack.inference.rules.plan.{SimplePlanGenerator, SimpleSQLGenerator, TriplesSchema}
import net.sansa_stack.inference.spark.backwardchaining.tree.{AndNode, OrNode}
import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader
import net.sansa_stack.inference.spark.data.model.RDFGraph
import net.sansa_stack.inference.utils.Logging
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.reasoner.rulesys.impl.BindingVector
import org.apache.jena.vocabulary.RDF

import net.sansa_stack.inference.utils.RuleUtils._
import net.sansa_stack.inference.utils.TripleUtils._
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rex.RexCall
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Project

/**
  * @author Lorenz Buehmann
  */
class BackwardChainingReasonerRDD(val rules: Set[Rule], val graph: RDFGraph) extends Logging{

  def isEntailed(triple: Triple): Boolean = {
    isEntailed(new TriplePattern(triple))
  }

  def isEntailed(tp: TriplePattern): Boolean = {

    val tree = buildTree(new AndNode(tp), Seq())
    println(tree.toString())

    val rdd = processTree(tree)
//    println(rdd.count())

    false
  }

  val planGenerator = new SimplePlanGenerator(TriplesSchema.get())

  private def processTree(tree: AndNode): RDD[Triple] = {
    // 1. look for asserted triples in the graph
    var rdd = graph.triples // lookup(tree.element)

    // 2. process the inference rules that can infer the triple pattern
    tree.children.foreach(child => {
      println(s"processing rule ${child.element}")

      processRule(child.element)

      val targetTp = child.element.headTriplePatterns().head

      // recursively process each instantiated body atom of the rule
      var node2RDD = child.children.map(
        c => (c, processTree(c))).toMap

      // and join them
      node2RDD.map(_._1).toList.combinations(2).foreach(pair => {
        val vars = joinVars(pair(0).element, pair(1).element)
        println(vars.mkString("\n"))
      })

      applyRule(child.element)
    })

    rdd
  }

  class RDDRelVisitor(rdd: RDD[Triple]) extends RelVisitor {
    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      println(node)

      val rdd = node match {
        case project: BindableProject =>


        case join: BindableJoin =>


        case filter: BindableFilter =>
          val operands = filter.getCondition.asInstanceOf[RexCall].getOperands

        case _ =>
      }

      super.visit(node, ordinal, parent)
    }

    override def go(node: RelNode): RelNode = super.go(node)
  }

  private def processRule(rule: Rule) = {
    val plan = planGenerator.generateLogicalPlan(rule)
    new RDDRelVisitor(graph.triples).go(plan)
  }

  private def selectedVars(body: TriplePattern, head: TriplePattern): Seq[Int] = {
    var selectedIndexes: Seq[Int] = Seq()

    val headVars = head.vars()

    if(headVars.contains(body.getSubject)) {
        selectedIndexes +:= 1
    }
    if(headVars.contains(body.getPredicate)) {
      selectedIndexes +:= 2
    }
    if(headVars.contains(body.getObject)) {
      selectedIndexes +:= 3
    }

    selectedIndexes
  }

  private def joinVars(tp1: TriplePattern, tp2: TriplePattern): Seq[(Node, Int, Int)] = {
    var joinVars: Seq[(Node, Int, Int)] = Seq()

    var tmp: Seq[(Node, Int)] = Seq()

    if(tp1.getSubject.isVariable) {
      tmp +:= (tp1.getSubject, 1)
    }
    if(tp1.getPredicate.isVariable) {
      tmp +:= (tp1.getPredicate, 2)
    }
    if(tp1.getObject.isVariable) {
      tmp +:= (tp1.getObject, 3)
    }

    tmp.foreach(entry => {
      val node = entry._1
      val index = entry._2
      if (tp2.getSubject.equals(node)) {
        joinVars +:= (node, index, 1)
      }
      if (tp2.getPredicate.equals(node)) {
        joinVars +:= (node, index, 2)
      }
      if (tp2.getObject.equals(node)) {
        joinVars +:= (node, index, 3)
      }
    })


    joinVars
  }

  private def lookup(tp: TriplePattern): RDD[Triple] = {
    graph.find(tp.asTriple())
  }

  private def buildTree(tree: AndNode, visited: Seq[Rule]): AndNode = {
    val tp = tree.element

    rules.filterNot(visited.contains(_)).foreach(r => {
      // check if the head is more general than the triple in question
      var head = r.headTriplePatterns()

      head.foreach(headTP => {
        val subsumes = headTP.subsumes(tp)

        if(subsumes) {
          // instantiate the rule
          val boundRule = instantiateRule(r, tp)

          // add new Or-node to tree
          val node = new OrNode(boundRule)
          //          println(node)
          tree.children :+= node

          boundRule.bodyTriplePatterns().foreach(newTp => {
            node.children :+= buildTree(new AndNode(newTp), visited ++ Seq(r))
          })
        }
      })

    })

    tree
  }

  /*
  // create a binding for the rule variables
   */
  private def instantiateRule(rule: Rule, tp: TriplePattern): Rule = {
    val headTP = rule.headTriplePatterns().head // TODO handle rules with multiple head TPs

    val binding = new BindingVector(5)

    // the subject
    if(tp.getSubject.isConcrete && headTP.getSubject.isVariable) {
      binding.bind(headTP.getSubject, tp.getSubject)
    }
    // the predicate
    if(tp.getPredicate.isConcrete && headTP.getPredicate.isVariable) {
      binding.bind(headTP.getPredicate, tp.getPredicate)
    }
    // the object
    if(tp.getObject.isConcrete && headTP.getObject.isVariable) {
      binding.bind(headTP.getObject, tp.getObject)
    }

    rule.instantiate(binding)
  }

  private def applyRule(rule: Rule) = {
    // convert to SQL
    val sqlGenerator = new SimpleSQLGenerator()
    val sql = sqlGenerator.generateSQLQuery(rule)

    // generate logical execution plan
    val planGenerator = new SimplePlanGenerator(TriplesSchema.get())
    val plan = planGenerator.generateLogicalPlan(rule)

    // apply plan
    plan.explain(new RelWriterImpl(new PrintWriter(System.out)))

  }


}

object BackwardChainingReasonerRDD {


  def main(args: Array[String]): Unit = {

    val parallelism = 20

    // the SPARK config
    val session = SparkSession.builder
      .appName(s"Spark Backward Chaining")
      .master("local[4]")
//      .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", parallelism)
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.shuffle.partitions", parallelism)
      .getOrCreate()

    val graph = RDFGraphLoader.loadFromDisk(session, args(0))

    val rules = RuleSets.RDFS_SIMPLE.filter(_.getName == "rdfs2")

    val tp = Triple.create(
      NodeFactory.createVariable("s"),
      RDF.`type`.asNode(),
      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person"))

    val reasoner = new BackwardChainingReasonerRDD(rules, graph)

    println(reasoner.isEntailed(tp))

    session.stop()
  }
}
