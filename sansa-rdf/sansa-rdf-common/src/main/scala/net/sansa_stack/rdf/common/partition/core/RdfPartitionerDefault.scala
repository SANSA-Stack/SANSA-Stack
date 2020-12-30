package net.sansa_stack.rdf.common.partition.core

import net.sansa_stack.rdf.common.partition.layout._
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.vocabulary.XSD

object RdfPartitionerDefault
  extends RdfPartitionerBase with Serializable {

  def determineLayoutDatatype(dtypeIri: String): TripleLayout = {
    val dti = if (dtypeIri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString") {
      XSD.xstring.getURI
    } else dtypeIri

    var v = TypeMapper.getInstance.getSafeTypeByName(dti).getJavaClass

    // type mapper returns null for some integer types
    if (v == null && intDTypeURIs.contains(dtypeIri)) v = classOf[Integer]

    // val v = node.getLiteralValue
    v match {
      case w if (w == classOf[java.lang.Byte] || w == classOf[java.lang.Short] || w == classOf[java.lang.Integer] || w == classOf[java.lang.Long]) => TripleLayoutLong
      case w if (w == classOf[java.lang.Float] || w == classOf[java.lang.Double]) => TripleLayoutDouble
      case w if dtypeIri == XSD.date.getURI => TripleLayoutStringDate
      // case w if(w == classOf[String]) => TripleLayoutString
      case w => TripleLayoutString
      // case _ => TripleLayoutStringDatatype

      // case _ => throw new RuntimeException("Unsupported object type: " + dtypeIri)
    }
  }
}
