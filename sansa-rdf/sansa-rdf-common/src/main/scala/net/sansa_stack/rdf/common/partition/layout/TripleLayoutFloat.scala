package net.sansa_stack.rdf.common.partition.layout

import scala.reflect.runtime.universe.{Type, typeOf}

import org.apache.jena.graph.Triple

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.SchemaStringFloat

object TripleLayoutFloat
  extends TripleLayout {
  override def schema: Type = typeOf[SchemaStringFloat]

  override def fromTriple(t: Triple): SchemaStringFloat = {
    val s = t.getSubject
    val o = t.getObject
    val v = if (o.isLiteral && o.getLiteralValue.isInstanceOf[Number]) {
      o.getLiteralValue.asInstanceOf[Number]
    } else throw new RuntimeException(s"Layout only for float values: $t")

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    SchemaStringFloat(sStr, v.floatValue())
  }
}
