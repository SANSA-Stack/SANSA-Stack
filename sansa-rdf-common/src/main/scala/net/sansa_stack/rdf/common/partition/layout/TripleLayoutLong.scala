package net.sansa_stack.rdf.common.partition.layout

import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.SchemaStringLong
import org.apache.jena.graph.Triple

object TripleLayoutLong
  extends TripleLayout {
  override def schema: Type = typeOf[SchemaStringLong]

  override def fromTriple(t: Triple): SchemaStringLong = {
    val s = t.getSubject
    val o = t.getObject
    val v = if (o.isLiteral() && o.getLiteralValue.isInstanceOf[Number]) {
      o.getLiteralValue.asInstanceOf[Number]
    } else throw new RuntimeException("Layout only for doubles" + t)

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    SchemaStringLong(sStr, v.longValue)
  }
}
