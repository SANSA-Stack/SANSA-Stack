package net.sansa_stack.rdf.common.partition.layout

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.SchemaStringGeometry
import org.apache.jena.geosparql.implementation.datatype.{GMLDatatype, WKTDatatype}
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
      val reader =
//        if (o.getLiteral.getDatatype == WKTDatatype.INSTANCE) {
        if (o.getLiteral.getDatatype.getURI == WKTDatatype.URI) {
          WKTReader.extract(o.getLiteralLexicalForm)
//        } else if (o.getLiteral.getDatatype == GMLDatatype.INSTANCE) {
        } else if (o.getLiteral.getDatatype.getURI == GMLDatatype.URI) {
          GMLReader.extract(o.getLiteralLexicalForm)
        } else {
          throw new RuntimeException(s"Unsupported datatype. Layout only for WKT or GML literals: $t")
        }
      reader.getGeometry
    } else {
      throw new RuntimeException(s"Not a literal node. Layout only for WKT or GML literals: $t")
    }

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    SchemaStringGeometry(sStr, v)
  }
}