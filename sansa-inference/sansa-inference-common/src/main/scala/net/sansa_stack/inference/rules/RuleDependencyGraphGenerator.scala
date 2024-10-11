package net.sansa_stack.inference.rules

import java.util.stream.Collectors

import scala.collection
import scala.jdk.CollectionConverters._
import scala.language.{existentials, implicitConversions}

import scalax.collection.GraphPredef._
import scalax.collection.GraphTraversal.Parameters
import scalax.collection._
import scalax.collection.edge.Implicits._
import scalax.collection.edge._
import scalax.collection.mutable.DefaultGraphImpl
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.vocabulary.RDFS
import org.jgrapht.alg.CycleDetector
import org.jgrapht.alg.cycle.TarjanSimpleCycles

import net.sansa_stack.inference.utils.RuleUtils._
import net.sansa_stack.inference.utils.graph.LabeledEdge
import net.sansa_stack.inference.utils.{GraphUtils, Logging, RuleUtils}
import collection.mutable.{ArrayBuffer, Buffer}
import scala.collection.mutable.HashMap

/**
  * A generator for a so-called dependency graph based on a given set of rules.
  * A dependency graph is a directed graph representing dependencies of several objects towards each other.
  * It is possible to derive an evaluation order or the absence of an evaluation order that respects the given
  * dependencies from the dependency graph.
  *
  * @author Lorenz Buehmann
  */
object RuleDependencyGraphGenerator extends Logging {

  sealed trait RuleDependencyDirection

  case object ConsumerProducer extends RuleDependencyDirection

  case object ProducerConsumer extends RuleDependencyDirection

  /**
    * Generates the rule dependency graph for a given set of rules.
    *
    * @param rules the set of rules
    * @param f     a function that denotes whether a rule `r1` depends on another rule `r2`
    * @return the rule dependency graph
    */
  def generate(rules: Set[Rule],
               f: (Rule, Rule) => Option[TriplePattern] = dependsOnSmart,
               pruned: Boolean = false,
               dependencyDirection: RuleDependencyDirection = ConsumerProducer): RuleDependencyGraph = {

    // create empty graph
    var g = new RuleDependencyGraph()

    // 1. add node for each rule
    rules.foreach(r => g.add(r))

    // 2. add edge for each rule r1 that depends on another rule r2
    for (r1 <- rules; r2 <- rules) {

      val r1r2 = f(r1, r2) // r1 depends on r2
      if (r1r2.isDefined) {
        val edgeLabel = r1r2.get
        val edge = dependencyDirection match {
          case ConsumerProducer => (r1 ~+> r2) (edgeLabel)
          case ProducerConsumer => (r2 ~+> r1) (edgeLabel)
        }
        g += edge
      } else {
        val r2r1 = f(r2, r1)
        if (r2r1.isDefined) { // r2 depends on r1
          val edgeLabel = r2r1.get
          val edge = dependencyDirection match {
            case ConsumerProducer => (r2 ~+> r1) (edgeLabel)
            case ProducerConsumer => (r1 ~+> r2) (edgeLabel)
          }
          g += edge
        }
      }

      // cycles?
      val r1r1 = f(r1, r1)
      if (r1r1.isDefined) { // r1 depends on r1, i.e. reflexive dependency
        g += (r1 ~+> r1) (r1r1.get)
      }
    }

    // 3. pruning
    val pruningRules = List(
      removeLoops _,
      removeEdgesWithPredicateAlreadyTC _,
      removeCyclesIfPredicateIsTC _,
      removeEdgesWithCycleOverTCNode _
      , removeCycles _
      , prune _
    )

//    for ((rule, i) <- pruningRules.zipWithIndex) g = {
//      println(s"$i." + "*" * 40)
//      rule.apply(g)
//    }

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
  def dependsOn(rule1: Rule, rule2: Rule): Boolean = {
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
  def dependsOnSmart(rule1: Rule, rule2: Rule): Option[TriplePattern] = {
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

        if (tp1.getPredicate.isVariable && tp2.getPredicate.isVariable) {
          ret = Some(tp2)
        }
      }

    }

    ret
  }

  def removeLoops(graph: RuleDependencyGraph): RuleDependencyGraph = {
    debug("removing non-TC loops")
    var edges2Remove = Seq[Graph[Rule, LDiEdge]#EdgeT]()

    graph.nodes.toSeq.foreach(node => {

      val loopEdge = node.outgoing.find(_.target == node)

      if (loopEdge.isDefined) {

        val edge = loopEdge.get

        val rule = node.value

        val isTC = RuleUtils.isTransitiveClosure(rule, edge.label.asInstanceOf[TriplePattern].getPredicate)

        if (!isTC) {
          edges2Remove :+= edge
          debug(s"loop of node $node")
        }
      }
    })

    val newNodes = graph.nodes.map(node => node.value)
    val newEdges = graph.edges.clone().filterNot(e => edges2Remove.contains(e)).map(edge => edge.toOuter)

    new RuleDependencyGraph(newNodes, newEdges)
  }

  def sameElements(this1: Traversable[_], that: Traversable[_]): Boolean = {
    this1.size == that.size && {
      val thisList = this1.to[List]
      // thisList.indexOf(that.head) may fail due to asymmetric equality
      val idx = thisList.indexWhere(_ == that.head)
      if (idx >= 0) {
        val thisDoubled = thisList ++ thisList.tail
        val thatList = that.to[List]
        (thisDoubled startsWith(thatList, idx)) ||
          (thisDoubled startsWith(thatList.reverse, idx))
      }
      else false
    }
  }

  def removeCycles(graph: RuleDependencyGraph): RuleDependencyGraph = {
    debug("removing redundant cycles")
    var edges2Remove = collection.mutable.Set[Graph[Rule, LDiEdge]#EdgeT]()

    graph.findCycle

    // convert to JGraphT graph for algorithms not contained in Scala Graph API
    val g = GraphUtils.asJGraphtRuleSetGraph(graph)


    val cycleDetector = new CycleDetector[Rule, LabeledEdge[Rule, TriplePattern]](g)

    val cycleDetector2 = new TarjanSimpleCycles[Rule, LabeledEdge[Rule, TriplePattern]](g)
    val allCycles = cycleDetector2.findSimpleCycles()

    graph.nodes.toSeq.foreach(node => {
      debug(s"NODE ${node.value}")

      // get cycles of length 3
      val cycles = cycleDetector.findCyclesContainingVertex(node.value)
      debug(cycles.asScala.mkString(","))

      // cycles that contain the current node
      val cyclesWithNode: Buffer[Buffer[Rule]] = allCycles.asScala.filter(cycle => cycle.contains(node.value)).map(cycle => cycle.asScala)

      // cycles that use the same property
      val cyclesWithNodeSameProp: Map[Node, scala.List[Buffer[graph.EdgeT]]] = cyclesWithNode.map(cycle => {

        debug("Cycle: " + cycle.mkString(", "))

        // pairs of rules (r1, r2)
        var pairsOfRules = cycle zip cycle.tail
        pairsOfRules :+= (cycle.last, cycle(0))

        // map to list of edges
        val edges: Buffer[graph.EdgeT] = pairsOfRules.flatMap(e => {
          val node1 = graph get e._1
          val node2 = graph get e._2

          node1.outgoing.filter(_.target == node2)
        })
        debug("Edges: " + edges.mkString(", "))

        // map to edge labels, i.e. the predicates
        var predicates = edges.map(_.label.asInstanceOf[TriplePattern].getPredicate)
        if(predicates.forall(_.isVariable)) predicates = ArrayBuffer(NodeFactory.createVariable("p"))
        debug("predicates:" + predicates)

        // return predicate if it's commonly used for all edges
        val samePred = predicates.size == 1
        if (samePred) Some(predicates(0), edges) else None
      }).filter(_.isDefined).map(_.get).groupBy(e => e._1).mapValues(e => e.map(x => x._2).toList)

      var removedCycles: collection.mutable.Set[Buffer[graph.EdgeT]] = collection.mutable.Set()

      val tmp: Map[Node, Map[Int, List[Buffer[graph.EdgeT]]]] = cyclesWithNodeSameProp.mapValues(value => value.map(cycle => (cycle.size, cycle)).groupBy(_._1).mapValues(e => e.map(x => x._2).toList))

        tmp.foreach(predicate2Cycles => {
          debug("predicate: " + predicate2Cycles._1)

          predicate2Cycles._2.foreach(entry => {
            debug(s"length ${entry._1}")

            val prop2Cycle = entry._2
            var pairsOfCycles = prop2Cycle zip prop2Cycle.tail
            pairsOfCycles.foreach(pair => {
              val cycle1 = pair._1
              val cycle2 = pair._2

              val cycle1Nodes = cycle1.map(_.source).toSet
              val cycle2Nodes = cycle2.map(_.source).toSet

              debug(cycle1Nodes.map(_.value.getName).mkString(", ") + " ???? " + cycle2Nodes.map(_.value.getName).mkString(", "))

              // check if both cycles contain the same nodes
              if(cycle1Nodes == cycle2Nodes) {
                debug("redundant cycle " + pair._1.map(_.source.value.getName))

                // we can remove cycle1 if cycle2 wasn't removed before
                if (!removedCycles.exists(c => sameElements(c, cycle2))) {
                  removedCycles += cycle1
                }
              }
            })
        })
      })

      removedCycles.map(c => c.map(_.asInstanceOf[Graph[Rule, LDiEdge]#EdgeT])).foreach(c =>
        {
          edges2Remove ++= c
        })


      // check for cycles over the same nodes via the same predicate in multiple directions
//      val grouped = cyclesWithNodeSameProp.groupBy(_._2)
//      grouped.foreach(e => {
//        debug(s"length ${e._1}")
//        val predicate2Cycles = e._2
//
//        predicate2Cycles.foreach(predicate2CyclesEntry => {
//          val prop2Cycle = predicate2CyclesEntry._2
//          var pairsOfCycles = prop2Cycle zip prop2Cycle.tail
//          pairsOfCycles.foreach(pair => {
//            debug(pair._1.map(_.source) + " ???? " + pair._2.map(_.source))
//
//            if(pair._1.map(_.source) == pair._2.map(_.source)) {
//              debug("redundant cycle " + pair._1.map(_.source.value.getName))
//            }
//          })
//        })
//
//
//      })

//      debug(cyclesWithNodeSameProp.map(prop2Cycle =>
//        s"${prop2Cycle._1} => ${prop2Cycle._2.map(edge => (edge.source.value.getName, edge.target.value.getName))}").mkString("\n"))

    })

    val newNodes = graph.nodes.map(node => node.value)
    val newEdges = graph.edges.clone().filterNot(e => edges2Remove.contains(e)).map(edge => edge.toOuter)

    new RuleDependencyGraph(newNodes, newEdges)
  }

  def prune(graph: RuleDependencyGraph): RuleDependencyGraph = {
    var redundantEdges = Seq[Graph[Rule, LDiEdge]#EdgeT]()

//    graph.outerNodeTraverser.foreach(n => println(n))

    // for each node n in G
    graph.nodes.toSeq.foreach(node => {
      debug("#" * 20)
      debug(s"NODE:${node.value.getName}")

      // get all direct successors
      var successors = node.innerNodeTraverser.withParameters(Parameters(maxDepth = 1)).toList
      // remove node itself, if it's a cyclic node
      successors = successors.filterNot(_.equals(node))
      debug(s"DIRECT SUCCESSORS:${successors.map(n => n.value.getName).mkString(", ")}")

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
          val path1 = node.withSubgraph(edges = e => !e.equals(edge2) && !redundantEdges.contains(e)) pathTo n2
          if (path1.isDefined) {
            debug(s"PATH TO ${n2.value.getName}: ${path1.get.edges.toList.map(edge => asString(edge))}")
            val edges = path1.get.edges.toList

            edges.foreach(edge => {
              debug(s"EDGE:${asString(edge)}")
            })
            val last = edges.last.value

            if (last.label == edge2.label) {
              debug(s"redundant edge $edge2")
              redundantEdges :+= edge2
            }
          } else {
            debug(s"NO OTHER PATH FROM ${node.value.getName} TO ${n2.value.getName}")
          }

          val path2 = node.withSubgraph(edges = e => !e.equals(edge1) && !redundantEdges.contains(e)) pathTo n1
          if (path2.isDefined) {
            debug(s"PATH TO:${n1.value.getName}")
            debug(s"PATH:${path2.get.edges.toList.map(edge => asString(edge))}")
            val edges = path2.get.edges.toList
            edges.foreach(edge => {
              debug(s"EDGE:${asString(edge)}")
            })
            val last = edges.last.value

            if (last.label == edge1.label) {
              debug(s"redundant edge $edge1")
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

  def removeRedundantEdges(graph: RuleDependencyGraph): RuleDependencyGraph = {
    debug("removeRedundantEdges")
    var redundantEdges = Seq[Graph[Rule, LDiEdge]#EdgeT]()

    // for each node n in G
    graph.nodes.toSeq.foreach(node => {
      debug("#" * 20)
      debug(s"NODE:${node.value.getName}")

      // check for nodes that do compute the TC
      val outgoingEdges = node.outgoing.withFilter(e => e.target != node)

      outgoingEdges.foreach(e => {
        val targetNode = e.target

      })
    })

    graph
  }

  // get all nodes that depend on a TC node for a predicate p and another node for p
  def removeEdgesWithPredicateAlreadyTC(graph: RuleDependencyGraph): RuleDependencyGraph = {
    debug("removeEdgesWithPredicateAlreadyTC")
    var redundantEdges = Seq[Graph[Rule, LDiEdge]#EdgeT]()

    // for each node n in G
    graph.nodes.toSeq.foreach(node => {
      debug("#" * 20)
      debug(s"NODE:${node.value.getName}")

      // check for nodes that do compute the TC
      val outgoingEdges = node.outgoing.withFilter(e => e.target != node)

      outgoingEdges.foreach(e => {
        val targetNode = e.target
        val rule = targetNode.value
        val edgeLabel = e.label
        val predicate = edgeLabel.asInstanceOf[TriplePattern].getPredicate
        // check if the target node computes the TC for the current edge predicate
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
    graph.nodes.toSeq.foreach(node => {
      debug("#" * 20)
      debug(s"NODE:${node.value.getName}")

      // get all nodes that depend on a TC node for a predicate p and produce p

      // get all successors
      val successors = node.innerNodeTraverser.filterNot(_.equals(node))
      debug(s"SUCCESSORS:${successors.map(n => n.value.getName)}")

      // check for nodes that do compute the TC
      successors.toSeq.foreach(n => {
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
    debug("removeCyclesIfPredicateIsTC")
    var redundantEdges = Seq[Graph[Rule, LDiEdge]#EdgeT]()

    // for each node n in G
    graph.nodes.toSeq.foreach(node => {
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
              debug(s"remove edge $edge")
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

  override def debug(msg: => String): Unit = println(msg)
}
