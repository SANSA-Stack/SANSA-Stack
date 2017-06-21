package net.sansa_stack.query.flink.sparqlify

import org.aksw.jena_sparql_api.views.E_RdfTerm
import org.aksw.sparqlify.util.SparqlifyCoreInit
import org.apache.jena.query.{Query, Syntax}
import org.apache.jena.sparql.ARQConstants
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.ExprVar
import org.apache.jena.sparql.util.ExprUtils
import org.scalatest.FlatSpec

/**
  * Created by Simon Bin on 21/06/17.
  */
class TestRdfTerm extends FlatSpec {

  "x" should "y" in {
    SparqlifyCoreInit.initSparqlifyFunctions()
    val term = E_RdfTerm.createVar(new ExprVar(Var.alloc("x")))
    //ARQConstants.getGlobalPrefixMap.setNsPrefix("sparqlify", "http://aksw.org/sparqlify/")
    println("Formatting result from object: " + ExprUtils.fmtSPARQL(term))
    val query = new Query()
    query.setSyntax(Syntax.syntaxARQ)
    query.setPrefixMapping(ARQConstants.getGlobalPrefixMap)
    val expr1 = ExprUtils.parse(query, "<http://aksw.org/sparqlify/rdfTerm>(-1, ?x, \"\", \"\")", true)
    println("formatting result: " + ExprUtils.fmtSPARQL(expr1))
    System.exit(0)
    val expr2 = ExprUtils.parse(term.toString)
    val expr3 = ExprUtils.parse(ExprUtils.fmtSPARQL(term))
    println(expr2)
    println(expr3)
  }
}
