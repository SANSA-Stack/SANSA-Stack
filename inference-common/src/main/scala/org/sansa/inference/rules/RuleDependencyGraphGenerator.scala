package org.sansa.inference.rules

import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.vocabulary.RDFS
import org.sansa.inference.utils.RuleUtils._

import scala.language.{existentials, implicitConversions}
import scalax.collection.GraphTraversal.{Parameters, Successors}
import scalax.collection.edge.Implicits._
import scalax.collection.GraphPredef._
import scalax.collection._
import GraphEdge._
import edge._
import edge.LBase._
import edge.Implicits._
import scalax.collection.immutable.DefaultGraphImpl

/**
  * A generator for a so-called dependency graph based on a given set of rules.
  * A dependency graph is a directed graph representing dependencies of several objects towards each other.
  * It is possible to derive an evaluation order or the absence of an evaluation order that respects the given
  * dependencies from the dependency graph.
  *
  * @author Lorenz Buehmann
  */
object RuleDependencyGraphGenerator {

  /**
    * Generates the rule dependency graph for a given set of rules.
    *
    * @param rules the set of rules
    * @param f a function that denotes whether a rule r1 depends on another rule r2
    * @return the rule dependency graph
    */
  def generate(rules: Set[Rule], f:(Rule, Rule) => Option[TriplePattern] = dependsOnSmart, pruned: Boolean = false): RuleDependencyGraph = {
    // create empty graph
    var g = new RuleDependencyGraph()

    // 1. add node for each rule
    rules.foreach(r => g.add(r))

    // 2. add edge for each rule r1 that depends on another rule r2
    for (r1 <- rules; r2 <- rules) {

      val r1r2 = f(r1, r2)// r1 depends on r2
      if (r1r2.isDefined)
        g += (r1 ~+> r2)(r1r2.get)
      else {
        val r2r1 = f(r2, r1)
        if (r2r1.isDefined) // r2 depends on r1
          g += (r2 ~+> r1)(r2r1.get)
      }
      val r1r1 = f(r1, r1)
      if (r1r1.isDefined) // r1 depends on r1, i.e. reflexive dependency
        g += (r1 ~+> r1)(r1r1.get)
    }

    // 3. pruning
    if(pruned) {
      g = prune(g)
    }

    g
  }

  /**
    * Checks whether rule `rule1` depends on rule `rule2`.
    * This methods currently checks if there is a triple pattern in the head of `rule2` that also occurs in the
    * body of `rule1`.
    *
    * @param rule1 the first rule
    * @param rule2 the second rule
    * @return whether the first rule depends on the second rule
    */
  def dependsOn(rule1: Rule, rule2: Rule) : Boolean = {
    // head of rule2
    val head2TriplePatterns = rule2.headTriplePatterns()
    // body of rule1
    val body1TriplePatterns = rule1.bodyTriplePatterns()

    var ret = false

    for (tp2 <- head2TriplePatterns; tp1 <- body1TriplePatterns) {
      if (tp2.getPredicate.equals(tp1.getPredicate)) { // matching predicates
        ret = true
      } else {
        if(tp1.getPredicate.isVariable && tp2.getPredicate.equals(RDFS.subPropertyOf.asNode())) {
          ret = true
        }
      }

    }

    ret
  }

  /**
    * Checks whether rule `rule1` depends on rule `rule2`.
    * This methods currently checks if there is a triple pattern in the head of `rule2` that also occurs in the
    * body of `rule1`.
    *
    * @param rule1 the first rule
    * @param rule2 the second rule
    * @return the triple pattern on which `rule1` depends
    */
  def dependsOnSmart(rule1: Rule, rule2: Rule) : Option[TriplePattern] = {
    // R1: B1 -> H1
    // R2: B2 -> H2
    // R2 -> R1 = ?  , i.e. H2 ∩ B1

    // head of rule2
    val head2TriplePatterns = rule2.headTriplePatterns()
    // body of rule1
    val body1TriplePatterns = rule1.bodyTriplePatterns()

    var ret: Option[TriplePattern] = None

    for (tp2 <- head2TriplePatterns; tp1 <- body1TriplePatterns) {
      // predicates are URIs
      if (tp2.getPredicate.equals(tp1.getPredicate)) { // matching predicates
        ret = Some(tp2)
      } else {
        if(tp1.getPredicate.isVariable && tp2.getPredicate.equals(RDFS.subPropertyOf.asNode())) {
          ret = Some(tp2)
        }

        if(tp1.getPredicate.isVariable && tp2.getPredicate.isVariable) {
          ret = Some(tp2)
        }
      }

    }

    ret
  }

  def prune(graph: RuleDependencyGraph): RuleDependencyGraph = {

    var redundantEdges = Seq[Graph[Rule, LDiEdge]#EdgeT]()

    // for each node n in G
    graph.nodes.foreach(node => {
      println("#"*20)
      println(s"NODE:${node.value.getName}")

      // get all direct successors
      var successors = node.innerNodeTraverser.withParameters(Parameters(maxDepth = 1)).toList
      // remove node itself, if it's a cyclic node
      successors = successors.filterNot(_.equals(node))
      println(s"SUCCESSORS:${successors.map(n => n.value.getName)}")

      if(successors.size > 1) {
        // get pairs of successors
        val pairs = successors zip successors.tail

        pairs.foreach(pair => {
          println(s"PAIR:${pair._1.value.getName},${pair._2.value.getName}")
          val n1 = pair._1
          val edge1 = node.innerEdgeTraverser.filter(e => e.source == node && e.target == n1).head
          val n2 = pair._2
          val edge2 = node.innerEdgeTraverser.filter(e => e.source == node && e.target == n2).head

          // n --p--> n1
          val path1 = node.withSubgraph(edges = !_.equals(edge2)) pathTo n2
          if(path1.isDefined) {
            println(s"PATH TO:${n2.value.getName}")
            println(s"PATH:${path1.get.edges.toList.map(edge => asString(edge))}")
            val edges = path1.get.edges.toList
            edges.foreach(edge => {
              println(s"EDGE:${asString(edge)}")
            })
            val last = edges.last.value

            if(last.label == edge2.label) {
              println("redundant")
              redundantEdges :+= edge2
            }
          } else {
            println(s"NO OTHER PATH FROM ${node.value.getName} TO ${n2.value.getName}")
          }

          val path2 = node.withSubgraph(edges = !_.equals(edge1))  pathTo n1
          if(path2.isDefined) {
            println(s"PATH TO:${n1.value.getName}")
            println(s"PATH:${path2.get.edges.toList.map(edge => asString(edge))}")
            val edges = path2.get.edges.toList
            edges.foreach(edge => {
              println(s"EDGE:${asString(edge)}")
            })
            val last = edges.last.value

            if(last.label == edge1.label) {
              println("redundant")
              redundantEdges :+= edge1
            }
          } else {
            println(s"NO OTHER PATH FROM ${node.value.getName} TO ${n1.value.getName}")
          }
        })
      }
    })

    val newNodes = graph.nodes.map(node => node.value)
    val newEdges = graph.edges.clone().filterNot(e => redundantEdges.contains(e)).map(edge => edge.toOuter)

    new RuleDependencyGraph(newNodes, newEdges)

  }

  def asString(edge: mutable.DefaultGraphImpl[Rule, LDiEdge]#EdgeT): String = {
    val e = edge.toOuter
    "[" + e.source.getName + " ~> " + e.target.getName + "] '" + e.label
  }
}
