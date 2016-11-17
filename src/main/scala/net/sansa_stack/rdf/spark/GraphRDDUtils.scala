package net.sansa_stack.rdf.spark

import java.util.ArrayList
import java.util.Arrays

import org.aksw.jena_sparql_api.utils.Vars
import org.aksw.jena_sparql_api.views.E_RdfTerm
import org.aksw.sparqlify.algebra.sql.nodes.SqlOpTable
import org.aksw.sparqlify.config.syntax.ViewDefinition
import org.aksw.sparqlify.config.syntax.ViewTemplateDefinition
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.Node
import org.apache.jena.graph.NodeFactory
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.Quad
import org.apache.jena.sparql.core.QuadPattern
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.E_Equals
import org.apache.jena.sparql.expr.Expr
import org.apache.jena.sparql.expr.ExprVar
import org.apache.spark.rdd.RDD

//import scala.reflect.runtime.{universe => ru}

//def getRetTypeOfMethod(tpe: ru.Type)(methodName: String) =
//  tpe.member(ru.TermName(methodName)).asMethod.returnType

trait TripleLayout {
  def schema: Class[_ <: Any]
  def fromTriple(t: Triple): Any
}


object TripleLayoutLong
  extends TripleLayout
{
  def schema = classOf[(String, Long)]

  def fromTriple(t: Triple): (String, Long) = {
    val s = t.getSubject
    val o = t.getObject
    val v = if(o.isLiteral() && o.getLiteralValue.isInstanceOf[Number])
       o.getLiteralValue.asInstanceOf[Number] else throw new RuntimeException("Layout only for doubles" + t)

    val sStr = RdfPartition.getUriOrBNodeString(s)

    (sStr, v.longValue)
  }
}



object TripleLayoutDouble
  extends TripleLayout
{
  def schema = classOf[(String, Double)]
  //val foo = universe

  def fromTriple(t: Triple): (String, Double) = {
    val s = t.getSubject
    val o = t.getObject
    val v = if(o.isLiteral() && o.getLiteralValue.isInstanceOf[Number])
       o.getLiteralValue.asInstanceOf[Number] else throw new RuntimeException("Layout only for doubles" + t)

    val sStr = RdfPartition.getUriOrBNodeString(s)

    (sStr, v.doubleValue)
  }
}

// Layout which can be used for blank nodes, IRIs, and plain iterals without language tag
object TripleLayoutString
  extends TripleLayout
{
  def schema = classOf[(String, String)]

  def fromTriple(t: Triple): (String, String) = {
    val s = t.getSubject
    val o = t.getObject

    val sStr = RdfPartition.getUriOrBNodeString(s)

    val result = if(o.isLiteral()) {
      (sStr, o.getLiteralLexicalForm)
    } else {
      val oStr = RdfPartition.getUriOrBNodeString(s)
      (sStr, oStr)
    }

    result
  }
}

// Layout for plain literals with language tag
object TripleLayoutStringLang
  extends TripleLayout
{
  def schema = classOf[(String, String, String)]

  def fromTriple(t: Triple): (String, String, String) = {
    val s = t.getSubject
    val o = t.getObject

    val sStr = RdfPartition.getUriOrBNodeString(s)

    val result = if(o.isLiteral()) {
      (sStr, o.getLiteralLexicalForm, o.getLiteralLanguage)
    } else {
      throw new RuntimeException("Layout only for literals")
    }

    result
  }
}

object SparqlifyUtils2 {
  implicit def newExprVar(varName: String): ExprVar = new ExprVar(Var.alloc(varName))
  implicit def newExprVar(varId: Int): ExprVar = "_" + varId



  def createViewDefinition(p: RdfPartition): ViewDefinition = {
        //val basicTableInfo = basicTableInfoProvider.getBasicTableInfo(sqlQueryStr)
        //println("Result schema: " + basicTableInfoProvider.getBasicTableInfo(sqlQueryStr))

        //items.foreach(x => println("Item: " + x))

        //println("Counting the dataset: " + ds.count())
        val pred = p.predicate
        val tableName = pred.substring(pred.lastIndexOf("/") + 1)
        val pn = NodeFactory.createURI(p.predicate)

        val quad = new Quad(Quad.defaultGraphIRI, Vars.s, pn, Vars.o)
        val quadPattern = new QuadPattern()
        quadPattern.add(quad)

        val sTerm = createExprForNode(0, p.subjectType, "", "")
        val oTerm = createExprForNode(1, p.objectType, p.datatype, p.langTag)

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

  def createExprForNode(offset: Int, termType: Byte, datatype: String, langTag: String): E_RdfTerm = {
    termType match {
      case 0 => E_RdfTerm.createBlankNode(offset)
      case 1 => E_RdfTerm.createUri(offset)
      case 2 if(!Option(langTag).getOrElse("").isEmpty) => E_RdfTerm.createPlainLiteral(offset, offset + 1)
      case 2 if(!Option(datatype).getOrElse("").isEmpty) => E_RdfTerm.createTypedLiteral(offset, offset + 1)
      case _ => throw new RuntimeException("Unhandled case")
    }
  }

}

//
/**
 * special datatypes: b for blank, u for uri, typed literal otherwise
 */
case class RdfPartition(val subjectType: Byte, val predicate: String, val objectType: Byte, val datatype: String, val langTag: String) {


  def matches(t: Triple): Boolean = {
    val p = RdfPartition.fromTriple(t)
    p == this
  }
}


object RdfPartition {
  def getUriOrBNodeString(node: Node): String = {
    val termType = getRdfTermType(node)
    termType match {
      case 0 => node.getBlankNodeId.getLabelString
      case 1 => node.getURI
      case _ => throw new RuntimeException("Neither Uri nor blank node: " + node)
    }
  }

  def getRdfTermType(node: Node): Byte = {
    val result =
      if(node.isURI()) 1.toByte else
      if(node.isLiteral()) 2.toByte else
      if(node.isBlank()) 0.toByte else
        throw new RuntimeException("Unknown RDF term type: " + node) //-1
    result
  }

  def fromTriple(t : Triple): RdfPartition = {
    println("TRIPLE: " + t)
    val s = t.getSubject
    val o = t.getObject

    val subjectType = getRdfTermType(s)
    val objectType = getRdfTermType(o)
    //val predicateType =

    val predicate = t.getPredicate.getURI
    val datatype = if(o.isLiteral()) o.getLiteralDatatypeURI else ""
    val langTag = if(o.isLiteral()) o.getLiteralLanguage else ""

    RdfPartition(subjectType, predicate, objectType, datatype, langTag)
  }


  /**
   * Lay a triple out based on the partition
   * Does not (re-)check the matches condition
   */
  def determineLayout(t: RdfPartition): TripleLayout = {
    val oType = t.objectType

    val layout = oType match {
      case 0 => TripleLayoutString
      case 1 => TripleLayoutString
      case 2 => if(t.objectType == 2 && t.datatype != "")
        determineLayoutDatatype(t.datatype)
        else if(t.langTag == "")
          TripleLayoutString else TripleLayoutStringLang
      case _ => throw new RuntimeException("Unsupported object type: " + t)
    }
    layout
  }

  def determineLayoutDatatype(dtypeIri: String): TripleLayout = {
    val v = TypeMapper.getInstance.getSafeTypeByName(dtypeIri).getJavaClass

    //val v = node.getLiteralValue
    v match {
      case w if(w == classOf[java.lang.Byte] || w == classOf[java.lang.Short] || w == classOf[java.lang.Integer] || w == classOf[java.lang.Long]) => TripleLayoutLong
      case w if(w == classOf[java.lang.Float] || w == classOf[java.lang.Double]) => TripleLayoutDouble
      case w if(w == classOf[String]) => TripleLayoutString
      case _ => throw new RuntimeException("Unsupported object type: " + dtypeIri)
    }
  }
}


object GraphRDDUtils extends Serializable {

  implicit def partitionGraphByPredicates(graphRdd : RDD[Triple]) : Map[RdfPartition, RDD[Any]] = {
    val map = Map(partitionGraphByPredicatesArray(graphRdd) :_*)
    map
  }

//  def equalsPredicate(a : Triple, b : Node) : Boolean = {
//    a.getPredicate == b
//  }

  implicit def partitionGraphByPredicatesArray(graphRdd : RDD[Triple]) : Array[(RdfPartition, RDD[Any])] = {
    //val predicates = graphRdd.map(_.getPredicate).distinct.map( _.getURI).collect
    val partitionKeys = graphRdd.map(RdfPartition.fromTriple).distinct.collect

    // TODO Collect an RDD of distinct with entries of structure (predicate, datatype, language tag)
    val array = partitionKeys map { p => (
          p,
          graphRdd
            .filter(p.matches) //_.getPredicate.getURI == p)
            .map(t => RdfPartition.determineLayout(p).fromTriple(t).asInstanceOf[Any])
            .persist())
          }
    array
//
//    val predicates = graphRdd.map(_.getPredicate).distinct.collect
//
//    val array = predicates map { p => (
//          p,
//          graphRdd
//            .filter(equalsPredicate(_, p))
//            .map(t => t.getSubject -> t.getObject)
//            .persist())
//          }
//    array
  }
}
