package net.sansa_stack.rdf.partition.layout

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

import org.apache.jena.graph.Triple

import net.sansa_stack.rdf.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.partition.schema.SchemaStringStringLang


// Layout for plain literals with language tag
object TripleLayoutStringLang
  extends TripleLayout
{
  override def schema = typeOf[SchemaStringStringLang]

  override def fromTriple(t: Triple): SchemaStringStringLang = {
    val s = t.getSubject
    val o = t.getObject

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    val result = if(o.isLiteral()) {
      SchemaStringStringLang(sStr, o.getLiteralLexicalForm, o.getLiteralLanguage)
    } else {
      throw new RuntimeException("Layout only for literals")
    }

    result
  }
}