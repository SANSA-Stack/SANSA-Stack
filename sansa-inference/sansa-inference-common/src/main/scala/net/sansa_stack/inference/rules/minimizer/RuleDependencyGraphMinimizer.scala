package net.sansa_stack.inference.rules.minimizer
import scala.collection.mutable.{ArrayBuffer, Buffer}

import scalax.collection.Graph
import scalax.collection.edge.LDiEdge
import scala.jdk.CollectionConverters._
import scala.collection.mutable

import scalax.collection.GraphTraversal.Parameters
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.jgrapht.alg.CycleDetector
import org.jgrapht.alg.cycle.TarjanSimpleCycles

import net.sansa_stack.inference.rules.RuleDependencyGraph
import net.sansa_stack.inference.rules.RuleDependencyGraphGenerator.{asString, debug, sameElements}
import net.sansa_stack.inference.utils.{GraphUtils, RuleUtils}
import net.sansa_stack.inference.utils.graph.LabeledEdge
import net.sansa_stack.inference.utils.RuleUtils._
import scalax.collection.GraphTraversal.Parameters
import scalax.collection._
import scalax.collection.edge.Implicits._
import scalax.collection.edge._
import scalax.collection.mutable.DefaultGraphImpl
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._

/**
  * @author Lorenz Buehmann
  */
abstract class RuleDependencyGraphMinimizer extends MinimizationRuleExecutor {

  def batches: Seq[Batch] = Seq(
    Batch("Default Minimization", Once,
      RemoveLoops,
            RemoveEdgesWithCycleOverTCNode,
            RemoveEdgesWithPredicateAlreadyTC,
            RemoveEdgeIfLongerPathToSameNodeExists,
      RemoveCyclesInBothDirections
//      RemoveCyclesIfPredicateIsTC,

    ))

  object RemoveLoops extends MinimizationRule {
    def apply(graph: RuleDependencyGraph): RuleDependencyGraph = {
      debug("removing non-TC loops")
      var edges2Remove = Seq[Graph[Rule, LDiEdge]#EdgeT]()

      graph.nodes.toSeq.foreach(node => {
        debug(s"node " + node.value.getName)

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
  }

  object RemoveCyclesIfPredicateIsTC extends MinimizationRule {
    def apply(graph: RuleDependencyGraph): RuleDependencyGraph = {
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
  }

  object RemoveEdgesWithPredicateAlreadyTC extends MinimizationRule {
    // get all nodes that depend on a TC node for a predicate p and another node for p
    def apply(graph: RuleDependencyGraph): RuleDependencyGraph = {
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
  }

  object RemoveEdgesWithCycleOverTCNode extends MinimizationRule {
    // for cycles x -p-> y -p-> z -s-> x with y being TC node for p, we can remove edge (z -s-> x)
    def apply(graph: RuleDependencyGraph): RuleDependencyGraph = {
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
  }


  object RemoveCyclesInBothDirections extends MinimizationRule {
    override def apply(graph: RuleDependencyGraph): RuleDependencyGraph = {
        debug("removing redundant cycles")
        var edges2Remove = collection.mutable.Set[Graph[Rule, LDiEdge]#EdgeT]()

        graph.findCycle

        // convert to JGraphT graph for algorithms not contained in Scala Graph API
        val g = GraphUtils.asJGraphtRuleSetGraph(graph)


        val cycleDetector = new CycleDetector[Rule, LabeledEdge[Rule, TriplePattern]](g)

        val cycleDetector2 = new TarjanSimpleCycles[Rule, LabeledEdge[Rule, TriplePattern]](g)
        val allCycles = cycleDetector2.findSimpleCycles()

        graph.nodes.toSeq.foreach(node => {
          debug(s"NODE ${node.value.getName}")

          // get cycles of length 3
//          val cycles = cycleDetector.findCyclesContainingVertex(node.value)
//          debug(cycles.asScala.mkString(","))

          // cycles that contain the current node
          val cyclesWithNode: mutable.Buffer[mutable.Buffer[Rule]] = allCycles.asScala
            .filter(cycle => cycle.contains(node.value))
            .map(cycle => cycle.asScala)
          debug("Cycles: " + cyclesWithNode.map(c => c.map(r => r.getName)).mkString(","))

          // cycles that use the same property
          val cyclesWithNodeSameProp: Map[Node, scala.List[mutable.Buffer[graph.EdgeT]]] = cyclesWithNode.map(cycle => {

            debug("Cycle: " + cycle.map(r => r.getName).mkString(", "))

            // pairs of rules (r1, r2)
            var pairsOfRules = cycle zip cycle.tail
            pairsOfRules :+= (cycle.last, cycle(0))

            // map to list of edges
            val edges: mutable.Buffer[graph.EdgeT] = pairsOfRules.flatMap(e => {
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

          var removedCycles: collection.mutable.Set[mutable.Buffer[graph.EdgeT]] = collection.mutable.Set()

          val tmp: Map[Node, Map[Int, List[mutable.Buffer[graph.EdgeT]]]] =
            cyclesWithNodeSameProp
              .mapValues(value =>
                value.map(cycle => (cycle.size, cycle))
                  .groupBy(_._1)
                  .mapValues(e => e.map(x => x._2)))

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

        })

        val newNodes = graph.nodes.map(node => node.value)
        val newEdges = graph.edges.clone().filterNot(e => edges2Remove.contains(e)).map(edge => edge.toOuter)

        new RuleDependencyGraph(newNodes, newEdges)
    }

    override def debug(msg: => String): Unit = println(msg)
  }

  object RemoveEdgeIfLongerPathToSameNodeExists extends MinimizationRule {
    def apply(graph: RuleDependencyGraph): RuleDependencyGraph = {
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
  }




}
