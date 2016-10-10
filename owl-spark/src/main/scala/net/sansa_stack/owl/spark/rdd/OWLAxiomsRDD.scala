package net.sansa_stack.owl.spark.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.io.OWLParserException
import org.semanticweb.owlapi.model.OWLAxiom


class OWLAxiomsPartition(override val index: Int) extends Partition


abstract class OWLAxiomsRDD(
                             sc: SparkContext,
                             override val parent: OWLFileRDD
                           ) extends RDD[OWLAxiom](sc, Nil) with OWLAxiomsRDDTrait


trait OWLAxiomsRDDTrait extends RDD[OWLAxiom] {
  def parent: OWLFileRDD
  def makeAxiom(expression: String): OWLAxiom

  override def compute(split: Partition, context: TaskContext): Iterator[OWLAxiom] = {
    parent.compute(split, context).map(line => {
      try makeAxiom(line)
      catch {
        case ex: OWLParserException => {
          log.warn("Parser error for line " + line + ": " + ex.getMessage)
          null
        }
      }
    })
  }

  override def getPartitions: Array[Partition] = {
    val numPartitions = parent.numPartitions
    val partitions = new Array[Partition](numPartitions)

    for (i <- 0 until numPartitions) {
      partitions(i) = new OWLAxiomsPartition(i)
    }

    partitions
  }
}