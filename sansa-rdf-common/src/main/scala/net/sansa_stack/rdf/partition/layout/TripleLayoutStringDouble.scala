package net.sansa_stack.rdf.partition.layout

import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf

import org.apache.jena.graph.Triple

import net.sansa_stack.rdf.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.partition.schema.SchemaStringDouble


object TripleLayoutDouble
  extends TripleLayout
{
  override def schema = typeOf[SchemaStringDouble]

  override def fromTriple(t: Triple): SchemaStringDouble = {
    val s = t.getSubject
    val o = t.getObject
    val v = if(o.isLiteral() && o.getLiteralValue.isInstanceOf[Number])
       o.getLiteralValue.asInstanceOf[Number] else throw new RuntimeException("Layout only for doubles: " + t)

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    SchemaStringDouble(sStr, v.doubleValue)
  }
}

