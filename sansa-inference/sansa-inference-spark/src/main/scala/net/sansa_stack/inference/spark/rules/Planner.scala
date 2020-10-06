package net.sansa_stack.inference.spark.rules

import scala.collection.mutable

import org.apache.jena.graph.Node
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.rdd.RDD

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.rules.plan.{Join, Plan}
import net.sansa_stack.inference.utils.RuleUtils._
import net.sansa_stack.inference.utils.TripleUtils._
import net.sansa_stack.inference.utils.{Logging, RuleUtils}

/**
  * @author Lorenz Buehmann
  */
object Planner extends Logging{

  /**
    * Generates an execution plan for a single rule.
    *
    * @param rule the rule
    */
  def generatePlan(rule: Rule): Plan = {
    info("Rule: " + rule)

    val body = rule.bodyTriplePatterns().map(tp => tp.toTriple).toSet

    val visited = mutable.Set[org.apache.jena.graph.Triple]()

//    process(body.head, body, visited)

    // group triple patterns by var
    val map = new mutable.HashMap[Node, collection.mutable.Set[org.apache.jena.graph.Triple]] () with mutable.MultiMap[Node, org.apache.jena.graph.Triple]
    body.foreach{tp =>
      val vars = RuleUtils.varsOf(tp)
      vars.foreach{v =>
        map.addBinding(v, tp)
      }
    }

    val joins = new mutable.HashSet[Join]

    map.foreach{e =>
      val v = e._1
      val tps = e._2.toList.sortBy(_.toString).combinations(2).foreach(c =>
        joins.add(new Join(c(0), c(1), v))
      )
    }

    new Plan(body, rule.headTriplePatterns().toList.head.asTriple(), joins)


//    val bodyGraph = RuleUtils.graphOfBody(rule)
//    println("Body graph:" + bodyGraph)
//
//    val headGraph = RuleUtils.graphOfHead(rule)
//
//    val headNodes = headGraph.nodes.toList
//
//    headNodes.foreach{node =>
//      if(node.value.isVariable) {
//        val bodyGraphNode = bodyGraph find node
//
//        bodyGraphNode match {
//          case Some(n) =>
//            val successor = n findSuccessor (_.outDegree > 0)
//
//            println("Node: " + n)
//            println("Out:" + n.outerEdgeTraverser.withDirection(Successors).toList)
//            println("In:" + n.outerEdgeTraverser.withDirection(Predecessors).toList)
//          case None => println("Not in body")
//        }
//
//      }
//    }

  }

  def process(tp: org.apache.jena.graph.Triple, body: mutable.ListBuffer[org.apache.jena.graph.Triple], visited: mutable.Set[org.apache.jena.graph.Triple]): Unit = {
    info("TP:" + tp)
    visited += tp

    // get vars of current triple pattern
    val vars = varsOf(tp)
    info("Vars: " + vars)

    // pick next connected triple pattern
    vars.foreach{v =>
      val nextTp = findNextTriplePattern(body, v)

      if(nextTp.isDefined) {
        val tp2 = nextTp.get
        info("Next TP:" + tp2)
        info(new Join(tp, tp2, v).toString)

        if(!visited.contains(tp2)) {
          process(tp2, body, visited)
        }
      }
    }
    body -= tp
  }

  def findNextTriplePattern(triplePatterns: mutable.Seq[org.apache.jena.graph.Triple], variable: Node): Option[org.apache.jena.graph.Triple] = {

    triplePatterns.foreach(tp => {
      tp.getPredicate.equals(variable)
    })
    val candidates = triplePatterns.filter(tp =>
        tp.getSubject.equals(variable) ||
        tp.getPredicate.equals(variable) ||
        tp.getObject.equals(variable))

    if(candidates.isEmpty) {
      None
    } else {
      Option(candidates.head)
    }
  }

  def toMultimap(triples: RDD[RDFTriple]): Unit = {

  }
}
