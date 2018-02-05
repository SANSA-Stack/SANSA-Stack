package net.sansa_stack.rdf.common.partition.layout

import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf


import org.apache.jena.graph.Triple
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.SchemaStringString


// Layout which can be used for blank nodes, IRIs, and plain iterals without language tag
object TripleLayoutString
  extends TripleLayout
{
  override def schema = typeOf[SchemaStringString]

  override def fromTriple(t: Triple): SchemaStringString = {
    val s = t.getSubject
    val o = t.getObject

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    val result = if(o.isLiteral()) {
      SchemaStringString(sStr, o.getLiteralLexicalForm)
    } else {
      val oStr = RdfPartitionerDefault.getUriOrBNodeString(o)
      SchemaStringString(sStr, oStr)
    }

    result
  }
}
