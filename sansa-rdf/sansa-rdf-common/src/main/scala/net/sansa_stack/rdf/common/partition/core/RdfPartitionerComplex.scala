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
class RdfPartitionerComplex(distinguishStringLiterals: Boolean = false, partitionPerLangTag: Boolean = true)
  extends RdfPartitionerBase(distinguishStringLiterals, partitionPerLangTag)
    with Serializable {

  override def determineLayoutDatatype(dtypeIri: String): TripleLayout = {
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

