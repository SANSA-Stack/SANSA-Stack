package net.sansa_stack.rdf.partition.sparqlify

import org.apache.jena.sparql.expr.ExprVar
import org.aksw.jena_sparql_api.views.E_RdfTerm
import org.apache.jena.sparql.core.QuadPattern
import org.apache.jena.sparql.expr.E_Equals
import org.aksw.sparqlify.config.syntax.ViewTemplateDefinition
import org.apache.jena.sparql.expr.NodeValue
import org.aksw.sparqlify.algebra.sql.nodes.SqlOpTable
import org.apache.jena.sparql.core.Var
import java.util.Arrays
import org.apache.jena.sparql.core.Quad
import org.aksw.jena_sparql_api.utils.Vars
import org.aksw.sparqlify.config.syntax.ViewDefinition
import java.util.ArrayList
import org.apache.jena.graph.NodeFactory
import net.sansa_stack.rdf.partition.core.RdfPartitionDefault
import org.apache.jena.sparql.expr.Expr

object SparqlifyUtils2 {
  implicit def newExprVar(varName: String): ExprVar = new ExprVar(Var.alloc(varName))
  implicit def newExprVar(varId: Int): ExprVar = "_" + varId

  def createViewDefinition(p: RdfPartitionDefault): ViewDefinition = {
    //val basicTableInfo = basicTableInfoProvider.getBasicTableInfo(sqlQueryStr)
    //println("Result schema: " + basicTableInfoProvider.getBasicTableInfo(sqlQueryStr))

    //items.foreach(x => println("Item: " + x))

    //println("Counting the dataset: " + ds.count())
    val pred = p.predicate
    val predPart = pred.substring(pred.lastIndexOf("/") + 1)
    val pn = NodeFactory.createURI(p.predicate)

    
    val dt = p.datatype
    val dtPart = if(dt != null && !dt.isEmpty) "_" + dt.substring(dt.lastIndexOf("/") + 1) else ""
    val langPart = if(p.langTagPresent) "_lang" else ""
    
    val tableName = predPart + dtPart.replace("#", "_") + langPart
    
    val quad = new Quad(Quad.defaultGraphIRI, Vars.s, pn, Vars.o)
    val quadPattern = new QuadPattern()
    quadPattern.add(quad)

    val sTerm = createExprForNode(0, p.subjectType, "", p.langTagPresent)
    val oTerm = createExprForNode(1, p.objectType, p.datatype, p.langTagPresent)

    val se = new E_Equals(new ExprVar(Vars.s), sTerm)
    val oe = new E_Equals(new ExprVar(Vars.o), oTerm)
    val varDefs = new ArrayList[Expr] //new ExprList()
    varDefs.add(se)
    varDefs.add(oe)

    //val typeMap = basicTableInfo.getRawTypeMap.asScala.map({ case (k, v) => (k, TypeToken.alloc(v)) }).asJava


    //val schema = new SchemaImpl(new ArrayList[String](basicTableInfo.getRawTypeMap.keySet()), typeMap)

    //println("Schema: " + schema)

    val sqlOp = new SqlOpTable(null, tableName)
    val vtd = new ViewTemplateDefinition(quadPattern, varDefs)
    val vd = new ViewDefinition(tableName, vtd, sqlOp, Arrays.asList())

    vd
  }

  def createExprForNode(offset: Int, termType: Byte, datatype: String, langTagPresent: Boolean): E_RdfTerm = {
    val o = offset + 1

    termType match {
      case 0 => E_RdfTerm.createBlankNode(o)
      case 1 => E_RdfTerm.createUri(o)
      case 2 => if(langTagPresent) E_RdfTerm.createPlainLiteral(o, o + 1) else E_RdfTerm.createTypedLiteral(o, NodeValue.makeString(datatype))
      //case 2 if(!Option(datatype).getOrElse("").isEmpty) => E_RdfTerm.createTypedLiteral(o, o + 1)
      case _ => throw new RuntimeException("Unhandled case")
    }
  }

}

