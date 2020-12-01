package net.sansa_stack.rdf.spark.partition.characteristc_sets

import org.apache.jena.graph.Node
import org.apache.jena.tdb.store.NodeType

case class RDFPartitionColumn(name: String, property: Node, columnType: NodeType)

case class RDFPartition(id: String, tableName: String, columns: Seq[RDFPartitionColumn]) {

}
