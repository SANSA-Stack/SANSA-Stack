package net.sansa_stack.inference.rules.plan

import scala.collection.mutable

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.reasoner.rulesys.Rule

import net.sansa_stack.inference.data.{SQLSchema, SQLSchemaDefault}
import net.sansa_stack.inference.utils.RuleUtils.RuleExtension
import net.sansa_stack.inference.utils.TripleUtils._
import net.sansa_stack.inference.utils.{Logging, RuleUtils, TripleUtils}

/**
  * A simple implementation of a SQL generator:
  * Joins are generated for common triple patterns in the body.
  * Projection variables are based on the head.
  * @author Lorenz Buehmann
  */
class SimpleSQLGenerator(val sqlSchema: SQLSchema = SQLSchemaDefault) extends SQLGenerator with Logging {

  val aliases = new mutable.HashMap[Triple, String]()
  var idx = 0

  def generateSQLQuery(rule: Rule): String = {
    debug(s"Rule:\n$rule")

    reset()

    val body = rule.bodyTriplePatterns().map(tp => tp.toTriple).toSet
    val head = rule.headTriplePatterns().head.asTriple()

    var sql = "SELECT "

    sql += projectionPart(body, head) + "\n"

    sql += fromPart(body) + "\n"

    sql += wherePart(body) + "\n"

    info(s"SQL Query:\n$sql")

    sql
  }

  private def reset(): Unit = {
    aliases.clear()
    idx = 0
  }

  private def determineJoins(triples: Set[Triple]): Set[Join] = {

    // group triple patterns by var
    val var2TPs = new mutable.HashMap[Node, collection.mutable.Set[org.apache.jena.graph.Triple]]() with mutable.MultiMap[Node, org.apache.jena.graph.Triple]
    triples.foreach { tp =>
      val vars = RuleUtils.varsOf(tp)
      vars.foreach { v =>
        var2TPs.addBinding(v, tp)
      }
    }

    val joins = var2TPs.flatMap{e =>
      val v = e._1

      e._2.toList.sortBy(_.toString()).combinations(2).map(c => new Join(c(0), c(1), v))
    }.toSet

    joins
  }

  private def projectionPart(body: Set[Triple], head: Triple): String = {
    var sql = ""

    val requiredVars = TripleUtils.nodes(head)

    val expressions = mutable.ArrayBuffer[String]()

    //    expressions += (if(target.getSubject.isVariable) expressionFor(target.getSubject, target) else target.getSubject.toString)
    //    expressions += (if(target.getPredicate.isVariable) expressionFor(target.getPredicate, target) else target.getPredicate.toString)
    //    expressions += (if(target.getObject.isVariable) expressionFor(target.getObject, target) else target.getObject.toString)

    var i = 0
    requiredVars.foreach{ v =>
      if (v.isVariable) {
        var done = false

        for(tp <- body; if !done) {
          val expr = expressionFor(v, tp)

          if(expr != "NULL") {
            expressions += withAlias(expr, i)
            done = true
          }
        }
      } else {
        val expr = "'" + v.toString() + "'"
        expressions += withAlias(expr, i)
      }
      i += 1
    }

    sql += expressions.mkString(", ")

    sql
  }

  private def withAlias(expr: String, i: Int): String = {
    val appendix = i match {
        case 0 => "s"
        case 1 => "p"
        case 2 => "o"
    }
    expr + " AS " + appendix
  }

  private def fromPart(body: Set[Triple]): String = {
    val joins = determineJoins(body)

    var sql = " FROM "

    // convert to list of pairs (1,2), (2,3), (3,4)
    val list = body.toList.sliding(2).collect { case List(a, b) => (a, b) }.toList

    val pair = list.head
    val tp1 = pair._1
    val tp2 = pair._2
    sql += fromPart(tp1) + " INNER JOIN " + fromPart(tp2) + " ON " + joinExpressionFor(joinsFor(tp1, tp2, joins)) + " "

    for (i <- 1 until list.length) {
      val pair = list(i)
      val tp1 = pair._1
      val tp2 = pair._2
      sql += " INNER JOIN " + fromPart(tp2) + " ON " + joinExpressionFor(joinsFor(tp1, tp2, joins)) + " "
    }


    //    sql += triplePatterns.map(tp => fromPart(tp)).mkString(" INNER JOIN ")
    //    sql += " ON " + joins.map(join => joinExpressionFor(join)).mkString(" AND ")
    sql
  }

  private def joinsFor(tp1: Triple, tp2: Triple, joins: Set[Join]): Join = {
    joins.filter(join => (join.tp1 == tp1 || join.tp2 == tp1) && (join.tp1 == tp2 || join.tp2 == tp2)).head
  }

  private def wherePart(body: Set[Triple]): String = {
    var sql = " WHERE "
    val expressions = mutable.ArrayBuffer[String]()

    expressions ++= body.flatMap(tp => whereParts(tp))
    //    expressions ++= joins.map(join => joinExpressionFor(join))

    sql += expressions.mkString(" AND ")

    sql
  }

  private def uniqueAliasFor(tp: Triple): String = {
    aliases.get(tp) match {
      case Some(alias) => alias
      case _ =>
        val alias = "rel" + idx
        aliases += tp -> alias
        idx += 1
        alias
    }
  }

  private def joinExpressionFor(join: Join): String = {
    expressionFor(join.joinVar, join.tp1) + "=" + expressionFor(join.joinVar, join.tp2)
  }

  private def fromPart(tp: Triple): String = {
    tableName(tp)
  }

  private def expressionFor(variable: Node, tp: Triple): String = {
      if (tp.subjectMatches(variable)) {
        subjectColumnName(tp)
      } else if (tp.predicateMatches(variable)) {
        predicateColumnName(tp)
      } else if (tp.objectMatches(variable)) {
        objectColumnName(tp)
      } else {
        "NULL"
      }
  }

  private def whereParts(tp: Triple): mutable.Set[String] = {
    val res = mutable.Set[String]()

    if (!tp.getSubject.isVariable) {
      res += subjectColumnName(tp) + "='" + tp.getSubject + "'"
    }

    if (!tp.getPredicate.isVariable) {
      res += predicateColumnName(tp) + "='" + tp.getPredicate + "'"
    }

    if (!tp.getObject.isVariable) {
      res += objectColumnName(tp) + "='" + tp.getObject + "'"
    }
    res
  }

  private def subjectColumnName(tp: Triple): String = {
    uniqueAliasFor(tp) + "." + sqlSchema.subjectCol
  }

  private def predicateColumnName(tp: Triple): String = {
    uniqueAliasFor(tp) + "." + sqlSchema.predicateCol
  }

  private def objectColumnName(tp: Triple): String = {
    uniqueAliasFor(tp) + "." + sqlSchema.objectCol
  }

  private def tableName(tp: Triple): String = {
    sqlSchema.triplesTable + " " + uniqueAliasFor(tp)
  }
}
