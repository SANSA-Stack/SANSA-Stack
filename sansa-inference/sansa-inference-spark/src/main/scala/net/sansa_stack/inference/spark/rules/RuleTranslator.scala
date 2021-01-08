package net.sansa_stack.inference.spark.rules

import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm
import edu.ucla.cs.wis.bigdatalog.interpreter.OperatorProgram
import edu.ucla.cs.wis.bigdatalog.system.{DeALSContext, ReturnStatus}
import org.apache.jena.assembler.RuleSet
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.sparql.core.Var

/**
 * @author Lorenz Buehmann
 */
object RuleTranslator {

  def main(args: Array[String]): Unit = {
    val ctx = new DeALSContext()
    ctx.initialize()

    val database =
      """database({
        |triple(S:string, P:string, O:string),
        |dom(p:string, C:string),
        |ran(p:string, C:string),
        |type(s:string, C:string),
        |sc(A:string, B:string)
        |}).
        |""".stripMargin



//    sc(A, C) <- sc(A, B), sc(B, C).
//    val program = "triple(S, 'type', C) <- triple(S, P, O), triple(P, 'domain', C). "
//    val query = "triple(S, P, O)."
    val program =
      """
        |type(S, C) <- triple(S, P, O), dom(P, C).
        |type(O, C) <- triple(S, P, O), ran(P, C).
        |
        |""".stripMargin

    val query = "type(S, C)."

    var db1 =
      """database({parent(X:string, Y:string)}).
        |""".stripMargin
    var p1 =
      """
        |ancestor(X, Y) <- parent(X, Y).
        |ancestor(X, Y) <- parent(X, Z), ancestor(Z, Y).
        |
        |export ancestor(X, Y).
        |""".stripMargin
    var q1 = "ancestor(X, Y)."

    db1 =
      """database({sc(X:string, Y:string)}).
        |""".stripMargin
    p1 =
      """
        |sc_tc(X, Y) <- sc(X, Y).
        |sc_tc(X, Y) <- sc_tc(X, Z), sc(Z, Y).
        |
        |export sc_tc(X, Y).
        |""".stripMargin
    q1 = "sc_tc(X, Y)."


    def execute(database: String, program: String, query: String) = {
      ctx.loadDatabaseObjects(database + program)

      val scr = ctx.compileQueryToOperators(query)
      if (scr.getStatus == ReturnStatus.SUCCESS) {
        val opProgram = scr.getObject.asInstanceOf[QueryForm].getProgram.asInstanceOf[OperatorProgram]

        println(opProgram)
      } else {
        System.err.println(scr.getMessage)
      }
    }

    execute(db1, p1, q1)

    val rulesStr =
      """
        |[rdfs2:  (?x ?p ?y), (?p rdfs:domain ?c) -> (?x rdf:type ?c)]
        |[rdfs3:  (?x ?p ?y), (?p rdfs:range ?c) -> (?y rdf:type ?c)]
        |[rdfs6:  (?a ?p ?b), (?p rdfs:subPropertyOf ?q) -> (?a ?q ?b)]
        |[rdfs9:  (?x rdfs:subClassOf ?y), (?a rdf:type ?x) -> (?a rdf:type ?y)]
        |""".stripMargin

    import scala.collection.JavaConverters._
    val rules = RuleSet.create(rulesStr)
    rules.getRules.asScala.foreach(rule => println(translate(rule)))
  }

  def translate(tp: TriplePattern): String = {
    val s = tp.getSubject
    val p = tp.getPredicate
    val o = tp.getObject

    var str = if (p.isVariable) "triple" else p.getURI
    str += "("
    str += (if (s.isVariable) s.getName.replace("?", "") else s"${s.toString}")
    str += ","
    str += (if (o.isVariable) o.getName.replace("?", "") else (if (o.isLiteral) s"${o.getLiteralLexicalForm}" else o.toString))
    str += ")"

    str
  }

  def translate(rule: Rule): String = {
    val head = rule.getHead
    var str = ""
    head.map(_.asInstanceOf[TriplePattern]).foreach(tp => {
      str += translate(tp)
      str += " <- "
      str += rule.getBody.map(_.asInstanceOf[TriplePattern]).map(translate).mkString(",")
      str += "."
    })
    str
  }

}
