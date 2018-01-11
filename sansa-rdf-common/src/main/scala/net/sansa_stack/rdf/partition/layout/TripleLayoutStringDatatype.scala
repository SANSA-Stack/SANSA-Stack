package net.sansa_stack.rdf.partition.layout

import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf


import org.apache.jena.graph.Triple
import net.sansa_stack.rdf.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.partition.schema.SchemaStringStringType


// Layout for custom datatypes - (subject, object lexical form, object datatype)
object TripleLayoutStringDatatype
  extends TripleLayout
{
  override def schema = typeOf[SchemaStringStringType]

  override def fromTriple(t: Triple): SchemaStringStringType = {
    val s = t.getSubject
    val o = t.getObject

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    val result = if(o.isLiteral()) {
      SchemaStringStringType(sStr, o.getLiteralLexicalForm, o.getLiteralDatatypeURI)
    } else {
      throw new RuntimeException("Layout only for literals")
    }

    result
  }

}