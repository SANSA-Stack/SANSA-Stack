package net.sansa_stack.rdf.common.partition.layout

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.SchemaStringGeometry
import org.apache.jena.geosparql.implementation.datatype.WKTDatatype
import org.apache.jena.geosparql.implementation.parsers.gml.GMLReader
import org.apache.jena.geosparql.implementation.parsers.wkt.WKTReader
import org.apache.jena.graph.Triple

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * A triple layout for WKT or GML literals.
 *
 * @author Lorenz Buehmann
 */
object TripleLayoutStringGeometry
  extends TripleLayout {
  override def schema: Type = typeOf[SchemaStringGeometry]

  override def fromTriple(t: Triple): SchemaStringGeometry = {
    val s = t.getSubject
    val o = t.getObject
    val v = if (o.isLiteral) {
      val reader = if (o.getLiteral.getDatatype == WKTDatatype.INSTANCE) {
        WKTReader.extract(o.getLiteralLexicalForm)
      } else {
        GMLReader.extract(o.getLiteralLexicalForm)
      }
      reader.getGeometry
    } else throw new RuntimeException(s"Layout only for WKT or GML literals: $t")

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    SchemaStringGeometry(sStr, v)
  }
}