package net.sansa_stack.rdf.common.partition.layout

import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.SchemaStringDouble
import org.apache.jena.graph.Triple

object TripleLayoutDouble
  extends TripleLayout {
  override def schema: Type = typeOf[SchemaStringDouble]

  override def fromTriple(t: Triple): SchemaStringDouble = {
    val s = t.getSubject
    val o = t.getObject
    val v = if (o.isLiteral && o.getLiteralValue.isInstanceOf[Number]) {
      o.getLiteralValue.asInstanceOf[Number]
    } else throw new RuntimeException("Layout only for doubles: " + t)

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    SchemaStringDouble(sStr, v.doubleValue)
  }
}
