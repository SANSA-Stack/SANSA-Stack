package net.sansa_stack.rdf.common.partition.layout

import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf


import org.apache.jena.graph.Triple
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault


// Layout for plain literals with language tag
object TripleLayoutStringLang
  extends TripleLayout
{
  def schema = typeOf[(String, String, String)]

  def fromTriple(t: Triple): (String, String, String) = {
    val s = t.getSubject
    val o = t.getObject

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    val result = if(o.isLiteral()) {
      (sStr, o.getLiteralLexicalForm, o.getLiteralLanguage)
    } else {
      throw new RuntimeException("Layout only for literals")
    }

    result
  }
}