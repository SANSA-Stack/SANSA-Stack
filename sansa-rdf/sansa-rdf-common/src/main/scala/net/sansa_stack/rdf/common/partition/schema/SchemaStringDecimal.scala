package net.sansa_stack.rdf.common.partition.schema

/**
 * Subject column is string and object column is a BigDecimal literal.
 *
 * @author Lorenz Buehmann
 */
case class SchemaStringDecimal(s: String, o: java.math.BigDecimal)
