package net.sansa_stack.owl.spark.rdd

import net.sansa_stack.owl.common.parsing.FunctionalSyntaxParsing
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.semanticweb.owlapi.io.OWLParserException
import org.semanticweb.owlapi.model.OWLAxiom


class FunctionalSyntaxOWLAxiomsRDD(
                                    @transient val sc: SparkContext,
                                    parent: OWLExpressionsRDD) extends OWLAxiomsRDD(sc, parent) with FunctionalSyntaxParsing {

  @transient private lazy val spark = {
    SparkSession.builder().appName(sc.appName).getOrCreate()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[OWLAxiom] = {
    parent.compute(split, context).map(line => {
      try makeAxiom(line)
      catch {
        case ex: OWLParserException => {
          log.warn("Parser error for line " + line + ": " + ex.getMessage)
          null
        }
      }
    }).filter(axiom => axiom != null)
  }

  override protected def getPartitions: Array[Partition] = {
    val numParentPartitions = parent.numPartitions
    val partitions = new Array[Partition](numParentPartitions)

    for (i <- 0 until numParentPartitions) {
      partitions(i) = new OWLAxiomsPartition(i)
    }

    partitions
  }

  override def asDataset: Dataset[OWLAxiom] = {
    // FIXME: Patrick: AFAIU this is the worst solution for serialization but there is not better one ATM
    implicit val enc = Encoders.kryo[OWLAxiom]
    spark.sqlContext.createDataset[OWLAxiom](this)
  }
}
