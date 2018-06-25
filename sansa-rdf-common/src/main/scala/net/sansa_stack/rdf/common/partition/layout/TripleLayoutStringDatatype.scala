package net.sansa_stack.rdf.common.partition.layout

import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.SchemaStringStringType
import org.apache.jena.graph.Triple

// Layout for custom datatypes - (subject, object lexical form, object datatype)
object TripleLayoutStringDatatype
  extends TripleLayout {
  override def schema: Type = typeOf[SchemaStringStringType]

  override def fromTriple(t: Triple): SchemaStringStringType = {
    val s = t.getSubject
    val o = t.getObject

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    val result = if (o.isLiteral()) {
      SchemaStringStringType(sStr, o.getLiteralLexicalForm, o.getLiteralDatatypeURI)
    } else {
      throw new RuntimeException("Layout only for literals")
    }

    result
  }

}
