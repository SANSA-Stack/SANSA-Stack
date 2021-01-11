package net.sansa_stack.rdf.common.partition.r2rml

import java.util

import com.google.common.collect.ImmutableMap
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner}
import net.sansa_stack.rdf.common.partition.model.sparqlify.SparqlifyUtils2.createExprForNode
import org.aksw.jena_sparql_api.utils.Vars
import org.aksw.obda.domain.api.Constraint
import org.aksw.obda.domain.impl.LogicalTableTableName
import org.aksw.r2rml.jena.domain.api.{ObjectMap, PredicateObjectMap, SubjectMap, TermMap, TriplesMap}
import org.aksw.r2rml.jena.vocab.RR
import org.aksw.r2rmlx.domain.api.TermMapX
import org.apache.jena.graph.NodeFactory
import org.apache.jena.rdf.model.{ModelFactory, ResourceFactory}
import org.apache.jena.sparql.core.{Quad, Var}
import org.apache.jena.sparql.expr.{Expr, ExprVar}

import scala.reflect.runtime.universe.MethodSymbol

object R2rmlUtils {
  implicit def newExprVar(varName: String): ExprVar = new ExprVar(Var.alloc(varName))
  implicit def newExprVar(varId: Int): ExprVar = "_" + varId

  def newExprVar(i: Int, attrNames: List[String]): ExprVar = {
    val attrName = attrNames(i)
    attrName
  }

  def createViewDefinition(partitioner: RdfPartitioner[RdfPartitionStateDefault], p: RdfPartitionStateDefault): TriplesMap = {
    // val basicTableInfo = basicTableInfoProvider.getBasicTableInfo(sqlQueryStr)
    // println("Result schema: " + basicTableInfoProvider.getBasicTableInfo(sqlQueryStr))

    // items.foreach(x => println("Item: " + x))

    val t = partitioner.determineLayout(p).schema
    // val t = p.layout.schema
    val attrNames = t.members.sorted.collect({ case m: MethodSymbol if m.isCaseAccessor => m.name.toString })

    // println("Counting the dataset: " + ds.count())
    val pred = p.predicate

    // For now let's just use the full predicate as the uri
    // val predPart = pred.substring(pred.lastIndexOf("/") + 1)
    val predPart = pred
    val pn = NodeFactory.createURI(p.predicate)

    val dt = p.datatype
    val dtPart = if (dt != null && dt.nonEmpty) "_" + dt.substring(dt.lastIndexOf("/") + 1) else ""
    val langPart = if (p.langTagPresent) "_lang" else ""

    val sTermTypePart = if (p.subjectType == 0) "sbn" else ""
    val oTermTypePart = if (p.objectType == 0) "obn" else ""

    val tableName = predPart + dtPart + langPart + sTermTypePart + oTermTypePart
    // .replace("#", "__").replace("-", "_")

    // TODO Ensure tableName is safe
    val tm: TriplesMap = ModelFactory.createDefaultModel.createResource.as(classOf[TriplesMap])
    val pom: PredicateObjectMap = tm.addNewPredicateObjectMap()
    pom.getPredicates.add(tm.getModel.wrapAsResource(pn))

    val sm: SubjectMap = tm.getOrSetSubjectMap()
    val om: ObjectMap = pom.addNewObjectMap()

    setTermMapForNode(sm, 0, attrNames, p.subjectType, "", false)
    setTermMapForNode(om, 1, attrNames, p.objectType, p.datatype, p.langTagPresent)

    tm.getOrSetLogicalTable().setTableName(tableName)

    tm
  }

  def setTermMapForNode(target: TermMap, offset: Int, attrNames: List[String], termType: Byte, datatype: String, langTagPresent: Boolean): TermMap = {
    // val o = offset + 1
    val o = offset

    val on = newExprVar(o, attrNames)

    termType match {
      // TODO The RR.IRI.inModel(...) is a workaround right now
      case 0 => target.setColumn(attrNames(o)).setDatatype(RR.BlankNode.inModel(target.getModel))
      case 1 => target.setColumn(attrNames(o)).setDatatype(RR.IRI.inModel(target.getModel))
      case 2 =>
        target.setColumn(attrNames(o))
        if (langTagPresent) {
          target.as(classOf[TermMapX]).setLangColumn(attrNames(o + 1))
        } else {
          target.setDatatype(ResourceFactory.createProperty(datatype))
        }
      // case 2 if(!Option(datatype).getOrElse("").isEmpty) => E_RdfTerm.createTypedLiteral(o, o + 1)
      case _ => throw new RuntimeException("Unhandled case")
    }

    target
  }
}
