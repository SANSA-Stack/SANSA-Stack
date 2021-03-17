package net.sansa_stack.rdf.common.partition.layout

import scala.reflect.runtime.universe.{Type, typeOf}
import org.apache.jena.graph.Triple
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.SchemaStringGeometry
import org.apache.jena.geosparql.implementation.parsers.wkt.WKTReader

/**
 * @author Lorenz Buehmann
 */
object TripleLayoutStringGeometry
  extends TripleLayout {
  override def schema: Type = typeOf[SchemaStringGeometry]

  override def fromTriple(t: Triple): SchemaStringGeometry = {
    val s = t.getSubject
    val o = t.getObject
    val v = if (o.isLiteral) {
      val wktReader = WKTReader.extract(o.getLiteralLexicalForm)
      wktReader.getGeometry
    } else throw new RuntimeException(s"Layout only for WKT geometry literals: $t")

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    SchemaStringGeometry(sStr, v)
  }
}