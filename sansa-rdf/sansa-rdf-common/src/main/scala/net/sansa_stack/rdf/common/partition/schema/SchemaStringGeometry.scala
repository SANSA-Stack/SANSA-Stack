package net.sansa_stack.rdf.common.partition.schema

import com.vividsolutions.jts.geom.Geometry

/**
 * Subject column is string and object column is geometry literal.
 *
 * @author Lorenz Buehmann
 */
case class SchemaStringGeometry(s: String, o: Geometry)
