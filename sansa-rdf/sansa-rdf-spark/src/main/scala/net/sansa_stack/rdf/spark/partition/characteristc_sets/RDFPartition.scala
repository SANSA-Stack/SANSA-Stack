package net.sansa_stack.rdf.spark.partition.characteristc_sets

import org.apache.jena.graph.Node
import org.apache.jena.tdb.store.NodeType

/**
 * A column of an RDF partition which refers to an RDF property in the dataset
 * @param name the name of the column in the corresponding Spark table
 * @param property the RDF property
 * @param columnType the type of the column, i.e. URI, bnode, literal
 */
case class RDFPartitionColumn(name: String, property: Node, columnType: NodeType)

/**
 * A partition that refers to a Spark Dataframe resp. its registred table
 * @param id the ID
 * @param tableName the Spark table name
 * @param columns the columns
 */
case class RDFPartition(id: String, tableName: String, columns: Seq[RDFPartitionColumn])


