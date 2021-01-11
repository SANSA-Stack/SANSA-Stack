package net.sansa_stack.rdf.common.partition.schema

/**
 * Subject column is string and object column is timestamp literal.
 *
 * @author Lorenz Buehmann
 */
case class SchemaStringTimestamp(s: String, o: java.sql.Timestamp)
