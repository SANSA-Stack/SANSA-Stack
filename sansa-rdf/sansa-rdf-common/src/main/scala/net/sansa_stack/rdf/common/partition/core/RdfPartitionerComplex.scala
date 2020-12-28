package net.sansa_stack.rdf.common.partition.core


import java.time.format.DateTimeFormatter

import net.sansa_stack.rdf.common.partition.layout.{TripleLayout, TripleLayoutBoolean, TripleLayoutDecimal, TripleLayoutDouble, TripleLayoutFloat, TripleLayoutLong, TripleLayoutString, TripleLayoutStringDate, TripleLayoutStringLang, TripleLayoutStringTimestamp}
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.vocabulary.{RDF, XSD}



/**
 * A partitioner for RDF that returns the corresponding layout given an RDF triple.
 * Parsing of `xsd:date` literals can be configured via constructor.
 *
 * @param distinguishStringLiterals if to create separate partitions for strings with and without language tags
// * @param dateFormatter parsing of literals with datatype `xsd:date` can be handled here. Default is a very
// *                      relaxed pattern `y-M-d` which also allows single digits for year, month and day though
// *                      officially according to XSD it must be `yyyy-MM-dd`
 */
class RdfPartitionerComplex(distinguishStringLiterals: Boolean = false)
  extends RdfPartitioner[RdfPartitionComplex]
    with Serializable {

  def getUriOrBNodeString(node: Node): String = {
    val termType = getRdfTermType(node)
    termType match {
      case 0 => node.getBlankNodeId.getLabelString
      case 1 => node.getURI
      case _ => throw new RuntimeException(s"Neither URI nor blank node: $node")
    }
  }

  def getRdfTermType(node: Node): Byte = {
    val result =
      if (node.isURI) 1.toByte else if (node.isLiteral) 2.toByte else if (node.isBlank) 0.toByte else {
        throw new RuntimeException(s"Unknown RDF term type: $node")
      } // -1
    result
  }

  def isPlainLiteralDatatype(dtypeIri: String): Boolean = {
    dtypeIri == null || dtypeIri == "" || dtypeIri == XSD.xstring.getURI || dtypeIri == RDF.langString.getURI
  }

  def isPlainLiteral(node: Node): Boolean = {
    node.isLiteral && isPlainLiteralDatatype(node.getLiteralDatatypeURI) // NodeUtils.isSimpleString(node) || NodeUtils.isLangString(node))
  }

  def isTypedLiteral(node: Node): Boolean = {
    node.isLiteral && !isPlainLiteral(node)
  }

  def fromTriple(t: Triple): RdfPartitionComplex = {
    val s = t.getSubject
    val o = t.getObject

    val subjectType = getRdfTermType(s)
    val objectType = getRdfTermType(o)
    // val predicateType =

    val predicate = t.getPredicate.getURI

    // In the case of plain literals, we replace the datatype langString with string
    // in order to group them all into the same partition
    val datatype = if (o.isLiteral) {
      if (!distinguishStringLiterals && isPlainLiteral(o)) XSD.xstring.getURI else o.getLiteralDatatypeURI
    } else {
      ""
    }
    val langTagPresent = o.isLiteral &&
                        ((distinguishStringLiterals && o.getLiteralDatatypeURI == RDF.langString.getURI && o.getLiteralLanguage.trim().nonEmpty)
                          || (!distinguishStringLiterals && isPlainLiteral(o)))

    val lang = if (langTagPresent) Some(o.getLiteralLanguage) else None

    RdfPartitionComplex(subjectType, predicate, objectType, datatype, langTagPresent, lang, this)
  }

  /**
   * Lay a triple out based on the partition
   * Does not (re-)check the matches condition
   */
  def determineLayout(t: RdfPartitionComplex): TripleLayout = {
    val oType = t.objectType

    val layout = oType match {
      case 0 | 1 => TripleLayoutString // URI or bnode
      case 2 => if (distinguishStringLiterals) {
        if (t.datatype == RDF.langString.getURI) {
          TripleLayoutStringLang
        } else if (t.datatype == XSD.xstring.getURI) {
          TripleLayoutString
        } else {
          determineLayoutDatatype(t.datatype)
        }
      } else {
        if (isPlainLiteralDatatype(t.datatype)) {
          TripleLayoutStringLang
        } else {
          determineLayoutDatatype(t.datatype)
        }
      }
      case _ => throw new RuntimeException(s"Unsupported object type: $t")
    }
    layout
  }

  private val intDTypeURIs = Set(XSDDatatype.XSDnegativeInteger, XSDDatatype.XSDpositiveInteger,
    XSDDatatype.XSDnonNegativeInteger, XSDDatatype.XSDnonPositiveInteger,
    XSDDatatype.XSDinteger, XSDDatatype.XSDint)
    .map(_.getURI)

  def determineLayoutDatatype(dtypeIri: String): TripleLayout = {
    val dti = if (dtypeIri == RDF.langString.getURI) XSD.xstring.getURI else dtypeIri

    var v = TypeMapper.getInstance.getSafeTypeByName(dti).getJavaClass

    // type mapper returns null for some integer types
    if (v == null && intDTypeURIs.contains(dtypeIri)) v = classOf[Integer]

    v match {
      case w if w == classOf[java.lang.Boolean] => TripleLayoutBoolean
      case w if w == classOf[java.lang.Byte] || w == classOf[java.lang.Short]
                || w == classOf[java.lang.Integer] || w == classOf[java.lang.Long] => TripleLayoutLong
      case w if w == classOf[java.lang.Double] => TripleLayoutDouble
      case w if w == classOf[java.lang.Float] => TripleLayoutFloat
      case w if w == classOf[java.math.BigDecimal] => TripleLayoutDecimal
      case w if w == classOf[java.math.BigInteger] => TripleLayoutLong // xsd:integer
      case w if dtypeIri == XSD.date.getURI => TripleLayoutStringDate // v will be null for xsd:date, thus, we have to compare the datatype URI
      case w if dtypeIri == XSD.dateTime.getURI || dtypeIri == XSD.dateTimeStamp.getURI => TripleLayoutStringTimestamp
      // case w if(w == classOf[String]) => TripleLayoutString
      case w => TripleLayoutString
      // case _ => TripleLayoutStringDatatype

      // case _ => throw new RuntimeException("Unsupported object type: " + dtypeIri)
    }
  }
}

object RdfPartitionerComplex {
  def apply(): RdfPartitionerComplex = new RdfPartitionerComplex()
  def apply(distinguishStringLiterals: Boolean): RdfPartitionerComplex = new RdfPartitionerComplex(distinguishStringLiterals)
}

