package net.sansa_stack.owl.spark.rdd

import net.sansa_stack.owl.common.parsing.FunctionalSyntaxInputFileIterator
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}


/**
  * Class handling OWL files in the functional syntax format.
  *
  * Will be the first step in the RDD processing chain
  *
  *   FunctionalSyntaxOWLFileRDD >> FunctionalSyntaxOWLAxiomsRDD
  *
  * with FunctionalSyntaxOWLAxiomsRDD being an OWLAxiomsRDD
  */
class FunctionalSyntaxOWLExpressionsRDD(
                                   sc: SparkContext,
                                   override val filePath: String,
                                   override val numPartitions: Int
                                 ) extends OWLExpressionsRDD(sc, filePath, numPartitions) {

  /** Iterator to read the input (functional syntax) file line by line */
  var inputFileIt: BufferedSource = null

  /**
    * Returns an iterator which
    * - reads the next functional syntax axiom definition (taking care of line
    *   breaks in case of multi-line string literals)
    * - skips the read axiom if it is not a multiple of the split number (i.e.
    *   axiomNumber % numPartitions != split.index)
    * - skips all comments and empty lines
    * - yields a string containing the next axiom (with
    *   axiomNumber % numPartitions == split.index)
    */
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {

    new FunctionalSyntaxInputFileIterator(filePath, numPartitions, split.index)
  }
}
