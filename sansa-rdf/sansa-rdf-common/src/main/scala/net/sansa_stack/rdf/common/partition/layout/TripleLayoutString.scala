package net.sansa_stack.rdf.common.partition.layout

import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.SchemaStringString
import org.apache.jena.graph.Triple

// Layout which can be used for blank nodes, IRIs, and plain iterals without language tag
object TripleLayoutString
  extends TripleLayout {
  override def schema: Type = typeOf[SchemaStringString]

  override def fromTriple(t: Triple): SchemaStringString = {
    val s = t.getSubject
    val o = t.getObject

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    val result = if (o.isLiteral) {
      SchemaStringString(sStr, o.getLiteralLexicalForm)
    } else {
      val oStr = RdfPartitionerDefault.getUriOrBNodeString(o)
      SchemaStringString(sStr, oStr)
    }

    result
  }
}
