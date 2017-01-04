package net.sansa_stack.inference.rules

import scala.collection.JavaConverters._
import scala.language.{existentials, implicitConversions}
import scalax.collection.GraphPredef._
import scalax.collection.GraphTraversal.Parameters
import scalax.collection._
import scalax.collection.edge.Implicits._
import scalax.collection.edge._

import org.apache.jena.graph.Node
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.vocabulary.RDFS
import org.jgrapht.alg.cycle.TarjanSimpleCycles

import net.sansa_stack.inference.utils.RuleUtils._
import net.sansa_stack.inference.utils.graph.LabeledEdge
import net.sansa_stack.inference.utils.{GraphUtils, Logging, RuleUtils}

/**
  * A generator for a so-called dependency graph based on a given set of rules.
  * A dependency graph is a directed graph representing dependencies of several objects towards each other.
  * It is possible to derive an evaluation order or the absence of an evaluation order that respects the given
  * dependencies from the dependency graph.
  *
  * @author Lorenz Buehmann
  */
object RuleDependencyGraphGenerator extends Logging {

  /**
    * Generates the rule dependency graph for a given set of rules.
    *
    * @param rules the set of rules
    * @param f a function that denotes whether a rule r1 depends on another rule r2
    * @return the rule dependency graph
    */
  def generate(rules: Set[Rule], f: (Rule, Rule) => Option[TriplePattern] = dependsOnSmart, pruned: Boolean = false): RuleDependencyGraph = {
    // create empty graph
    var g = new RuleDependencyGraph()

    // 1. add node for each rule
    rules.foreach(r => g.add(r))

    // 2. add edge for each rule r1 that depends on another rule r2
    for (r1 <- rules; r2 <- rules) {

      val r1r2 = f(r1, r2) // r1 depends on r2
      if (r1r2.isDefined) {
        g += (r1 ~+> r2) (r1r2.get)
      } else {
        val r2r1 = f(r2, r1)
        if (r2r1.isDefined) { // r2 depends on r1
          g += (r2 ~+> r1) (r2r1.get)
        }
      }
      val r1r1 = f(r1, r1)
      if (r1r1.isDefined) { // r1 depends on r1, i.e. reflexive dependency
        g += (r1 ~+> r1) (r1r1.get)
      }
    }

    // 3. pruning
    if (pruned) {
      g = removeEdgesWithPredicateAlreadyTC(g)
      g = removeCyclesIfPredicateIsTC(g)
      g = removeEdgesWithCycleOverTCNode(g)
      //      g = prune(g)
      //      g = prune1(g)
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
        if (tp1.getPredicate.isVariable && tp2.getPredicate.equals(RDFS.subPropertyOf.asNode())) {
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
        if (tp1.getPredicate.isVariable && tp2.getPredicate.equals(RDFS.subPropertyOf.asNode())) {
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
      debug("#" * 20)
      debug(s"NODE:${node.value.getName}")

      // get all direct successors
      var successors = node.innerNodeTraverser.withParameters(Parameters(maxDepth = 1)).toList
      // remove node itself, if it's a cyclic node
      successors = successors.filterNot(_.equals(node))
      debug(s"SUCCESSORS:${successors.map(n => n.value.getName)}")

      if (successors.size > 1) {
        // get pairs of successors
        val pairs = successors zip successors.tail

        pairs.foreach(pair => {
          debug(s"PAIR:${pair._1.value.getName},${pair._2.value.getName}")
          val n1 = pair._1
          val edge1 = node.innerEdgeTraverser.filter(e => e.source == node && e.target == n1).head
          val n2 = pair._2
          val edge2 = node.innerEdgeTraverser.filter(e => e.source == node && e.target == n2).head

          // n --p--> n1
          val path1 = node.withSubgraph(edges = !_.equals(edge2)) pathTo n2
          if (path1.isDefined) {
            debug(s"PATH TO:${n2.value.getName}")
            debug(s"PATH:${path1.get.edges.toList.map(edge => asString(edge))}")
            val edges = path1.get.edges.toList
            edges.foreach(edge => {
              debug(s"EDGE:${asString(edge)}")
            })
            val last = edges.last.value

            if (last.label == edge2.label) {
              debug("redundant")
              redundantEdges :+= edge2
            }
          } else {
            debug(s"NO OTHER PATH FROM ${node.value.getName} TO ${n2.value.getName}")
          }

          val path2 = node.withSubgraph(edges = !_.equals(edge1)) pathTo n1
          if (path2.isDefined) {
            debug(s"PATH TO:${n1.value.getName}")
            debug(s"PATH:${path2.get.edges.toList.map(edge => asString(edge))}")
            val edges = path2.get.edges.toList
            edges.foreach(edge => {
              debug(s"EDGE:${asString(edge)}")
            })
            val last = edges.last.value

            if (last.label == edge1.label) {
              debug("redundant")
              redundantEdges :+= edge1
            }
          } else {
            debug(s"NO OTHER PATH FROM ${node.value.getName} TO ${n1.value.getName}")
          }
        })
      }
    })

    val newNodes = graph.nodes.map(node => node.value)
    val newEdges = graph.edges.clone().filterNot(e => redundantEdges.contains(e)).map(edge => edge.toOuter)

    new RuleDependencyGraph(newNodes, newEdges)

  }

  // get all nodes that depend on a TC node for a predicate p and another node for p
  def removeEdgesWithPredicateAlreadyTC(graph: RuleDependencyGraph): RuleDependencyGraph = {
    debug("removeEdgesWithPredicateAlreadyTC")
    var redundantEdges = Seq[Graph[Rule, LDiEdge]#EdgeT]()

    // for each node n in G
    graph.nodes.foreach(node => {
      debug("#" * 20)
      debug(s"NODE:${node.value.getName}")

      // check for nodes that do compute the TC
      val outgoingEdges = node.outgoing.withFilter(e => e.target != node)

      outgoingEdges.foreach(e => {
        val targetNode = e.target
        val rule = targetNode.value
        val predicate = e.label.asInstanceOf[TriplePattern].getPredicate
        val isTCNode = RuleUtils.isTransitiveClosure(rule, predicate)
        debug(s"Direct successor:${rule.getName}\t\tisTC = $isTCNode")

        // if it depends on a TC node
        if(isTCNode) {
          // check for dependency on other nodes that produce the same predicate
          val samePredicateEdges = outgoingEdges
            .withFilter(e2 => e != e2)
            .withFilter(e2 => e2.label.asInstanceOf[TriplePattern].getPredicate.matches(predicate))
          debug(s"Redundant edges:${samePredicateEdges.map(e => e.toOuter.source.value.getName + "->" + e.toOuter.target.value.getName)}")
          redundantEdges ++:= samePredicateEdges

        }

      })


    })

    val newNodes = graph.nodes.map(node => node.value)
    val newEdges = graph.edges.clone().filterNot(e => redundantEdges.contains(e)).map(edge => edge.toOuter)

    new RuleDependencyGraph(newNodes, newEdges)

  }

  // for cycles x -p-> y -p-> z -s-> x with y being TC node for p, we can remove edge (z -s-> x)
  def removeEdgesWithCycleOverTCNode(graph: RuleDependencyGraph): RuleDependencyGraph = {
    debug("removeEdgesWithCycleOverTCNode")

    var redundantEdges = Seq[Graph[Rule, LDiEdge]#EdgeT]()

    // convert to JGraphT graph for algorithms not contained in Scala Graph API
    val g = GraphUtils.asJGraphtRuleSetGraph(graph)

    // get cycles of length 3
    val cycleDetector = new TarjanSimpleCycles[Rule, LabeledEdge[Rule, TriplePattern]](g)
    val cycles = cycleDetector.findSimpleCycles().asScala.filter(c => c.size() == 3)

    cycles.foreach(c => {
      debug(s"cycle:$c")

      val pathNodes = c.asScala.map(n => graph get n)

      // get nodes that are TC with same in and out edge
      val anchorNodes = pathNodes.filter(n => {
        // in and out edge with same predicate
        val inPred = n.incoming.head.label.asInstanceOf[TriplePattern].getPredicate
        val outPred = n.outgoing.head.label.asInstanceOf[TriplePattern].getPredicate

        inPred.matches(outPred) && RuleUtils.isTransitiveClosure(n.value, inPred)
      })

      if(anchorNodes.size == 1) {
        val anchor = anchorNodes.head
        // remove edge between two other nodes
        val edge = pathNodes.indexOf(anchor) match {
          case 0 => pathNodes(1).outgoing.filter(e => e.target == pathNodes(2)).head
          case 1 => pathNodes(2).outgoing.filter(e => e.target == pathNodes(0)).head
          case 2 => pathNodes(0).outgoing.filter(e => e.target == pathNodes(1)).head
        }
        debug(s"Redundant edge:${edge}")
        redundantEdges +:= edge

      }


    })

    val newNodes = graph.nodes.map(node => node.value)
    val newEdges = graph.edges.clone().filterNot(e => redundantEdges.contains(e)).map(edge => edge.toOuter)

    new RuleDependencyGraph(newNodes, newEdges)

  }

  def prune1(graph: RuleDependencyGraph): RuleDependencyGraph = {

    var redundantEdges = Seq[Graph[Rule, LDiEdge]#EdgeT]()

    // for each node n in G
    graph.nodes.foreach(node => {
      debug("#" * 20)
      debug(s"NODE:${node.value.getName}")

      // get all nodes that depend on a TC node for a predicate p and produce p

      // get all successors
      val successors = node.innerNodeTraverser.filterNot(_.equals(node))
      debug(s"SUCCESSORS:${successors.map(n => n.value.getName)}")

      // check for nodes that do compute the TC
      successors.foreach(n => {
        debug(s"successor:${n.value.getName}")
        val rule = n.value
        val edges = node.innerEdgeTraverser.filter(e => e.target == n && e.source != n)

        edges.foreach(e => {
          // the predicate that is produced from the successor
          val predicate = e.label.asInstanceOf[TriplePattern].getPredicate
          debug(s"predicate:$predicate")

          // is the successor a TC node for that predicate
          val isTC = RuleUtils.isTransitiveClosure(rule, predicate)
          debug(isTC.toString)

          if(isTC) {
            // remove edges that produce the same predicate
            val predecessors = node.innerEdgeTraverser.filter(e => e.target == node)

           predecessors
              .filter(inEdge => inEdge.label.asInstanceOf[TriplePattern].getPredicate.matches(predicate))
              .foreach{
                inEdge => {
                debug(s"remove edge$inEdge")
                redundantEdges +:= inEdge
              }
            }
          }
        })

      })

    })

    val newNodes = graph.nodes.map(node => node.value)
    val newEdges = graph.edges.clone().filterNot(e => redundantEdges.contains(e)).map(edge => edge.toOuter)

    new RuleDependencyGraph(newNodes, newEdges)

  }

  def removeCyclesIfPredicateIsTC(graph: RuleDependencyGraph): RuleDependencyGraph = {

    var redundantEdges = Seq[Graph[Rule, LDiEdge]#EdgeT]()

    // for each node n in G
    graph.nodes.foreach(node => {
      val rule = node.value

      // we only handle cyclic rules
      if(node.innerEdgeTraverser.exists(e => e.source == node && e.target == node)) {
        debug("#" * 20)
        debug(s"NODE:${node.value.getName}")
        debug(s"Rule:${node.value}")

        val bodyTPs = rule.bodyTriplePatterns()
        val headTPs = rule.headTriplePatterns()

        // for now we assume only 1 TP in head
        if(headTPs.size > 1) {
          throw new RuntimeException("Rules with more than 1 triple pattern in head not supported yet!")
        }
        val head = headTPs.head

        // transform to graph
        val ruleGraph = RuleUtils.asGraph(rule)

        val subjectNode = ruleGraph.get(head.getSubject)
        val objectNode = ruleGraph.get(head.getObject)
        val headEdge = subjectNode.innerEdgeTraverser.filter(e => e.target == objectNode && e.label == head.getPredicate).head

        // check if there is a path in body from the same subject to the same object
        val pathOpt = subjectNode.withSubgraph(edges = !_.equals(headEdge)) pathTo objectNode
        debug(pathOpt.toString)

        // check if there is some other triple pattern in body
        if(pathOpt.isDefined) {
          val path = pathOpt.get
          val predicateOpt: Option[Node] = path.length match {
            case 1 =>
              val p1 = path.edges.head.label.asInstanceOf[Node]
              val p2 = headEdge.label.asInstanceOf[Node]
              val p1Node = ruleGraph.get(p1)
              val p2Node = ruleGraph.get(p2)

              val pEdge = ruleGraph.edges.filter(e => e.source == p1Node && e.target == p2Node).head
              Some(pEdge.label.asInstanceOf[Node])
            case 2 =>
              val otherEdges = path.edges.filterNot(e => e.label == headEdge.label)

              if(otherEdges.nonEmpty) {
                Some(otherEdges.head.label.asInstanceOf[Node])
              } else {
                None
              }
            case _ => None
          }

          if(predicateOpt.isDefined) {
            val predicate = predicateOpt.get
            debug(s"Predicate:$predicate")

            // check if predicate TC will be materialized before in the RDG
            val tcMaterialized = node.innerNodeTraverser.filter(n => {

              n.value.headTriplePatterns().exists(tp => tp.getPredicate.matches(predicate)) &&
                (n.innerEdgeTraverser.exists(e => e == LDiEdge(n, n)(predicate)) ||  n.findCycle.isDefined)
            })

            if(tcMaterialized.nonEmpty) {
              debug(s"$predicate already materialized in node(s) ${tcMaterialized.map(n => n.value.getName)}")
              val edge = node.innerEdgeTraverser.filter(e =>
                e.source == node &&
                  e.target == node &&
                  e.label.asInstanceOf[TriplePattern].equals(head)
              ).head
              //            val edge = (node ~+> node)(head)
              redundantEdges +:= edge
            }

          }

        }
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
