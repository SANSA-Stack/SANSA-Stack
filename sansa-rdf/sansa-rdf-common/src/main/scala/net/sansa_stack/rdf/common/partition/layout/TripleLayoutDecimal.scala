package net.sansa_stack.rdf.common.partition.layout


import scala.reflect.runtime.universe.{Type, typeOf}

import org.apache.jena.graph.Triple

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.SchemaStringDecimal

object TripleLayoutDecimal
  extends TripleLayout {
  override def schema: Type = typeOf[SchemaStringDecimal]

  override def fromTriple(t: Triple): SchemaStringDecimal = {
    val s = t.getSubject
    val o = t.getObject
    val v = if (o.isLiteral && o.getLiteral.getDatatype.getJavaClass == classOf[java.math.BigDecimal]) {
      o.getLiteralValue match {
        case value: Integer => java.math.BigDecimal.valueOf(value.longValue()) // Jena parses e.g. 1.0 to Integer
        case value: java.lang.Long => java.math.BigDecimal.valueOf(value.longValue())
        case value: java.math.BigInteger => new java.math.BigDecimal(value)
        case _ => o.getLiteralValue.asInstanceOf[java.math.BigDecimal]
      }
    } else throw new RuntimeException(s"Layout only for BigDecimal values: $t")

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    SchemaStringDecimal(sStr, v)
  }
}
