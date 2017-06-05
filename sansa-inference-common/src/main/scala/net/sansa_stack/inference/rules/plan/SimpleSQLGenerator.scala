package net.sansa_stack.inference.rules.plan

import scala.collection.mutable

import org.apache.jena.graph.Node
import org.apache.jena.reasoner.rulesys.Rule

import net.sansa_stack.inference.utils.RuleUtils.RuleExtension
import net.sansa_stack.inference.utils.TripleUtils._
import net.sansa_stack.inference.utils.{Logging, RuleUtils}

/**
  * @author Lorenz Buehmann
  */
class SimpleSQLGenerator extends SQLGenerator with Logging {

  def generateSQLQuery(rule: Rule): String = {
    info(s"Rule:\n$rule")

    val body = rule.bodyTriplePatterns().map(tp => tp.toTriple).toSet

    val visited = mutable.Set[org.apache.jena.graph.Triple]()

    //    process(body.head, body, visited)

    // group triple patterns by var
    val map = new mutable.HashMap[Node, collection.mutable.Set[org.apache.jena.graph.Triple]]() with mutable.MultiMap[Node, org.apache.jena.graph.Triple]
    body.foreach { tp =>
      val vars = RuleUtils.varsOf(tp)
      vars.foreach { v =>
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

    val sqlQuery = new Plan(body, rule.headTriplePatterns().toList.head.asTriple(), joins).toSQL
    info(s"SQL Query:\n$sqlQuery")

    sqlQuery
  }
}
