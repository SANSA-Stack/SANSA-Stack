package net.sansa_stack.rdf.spark.partition.semantic

import java.util.concurrent.TimeUnit
import org.apache.jena.graph.Triple
import org.apache.spark.rdd._

/*
 * RdfPartition - semantic partition of and RDF graph
 * @symbol - list of symbols.
 * @nTroplesRDD - a RDD of n-triples.
 * @return - semantic partition data.
 */
class RdfPartition(
  symbol:              Map[String, String],
  nTriplesRDD:         RDD[Triple],
  partitionedDataPath: String,
  numOfFilesPartition: Int) extends Serializable {

  def run() = {
    // start process time
    val startTime = System.nanoTime()

    // execute partition
    val partitionedData = partitionGraph()

    // end process time
    this.partitionTime(System.nanoTime() - startTime)

    // save data to output file
    if (partitionedData.partitions.nonEmpty) {
      partitionedData.repartition(this.numOfFilesPartition).saveAsTextFile(this.partitionedDataPath)
    }
    partitionedData
  }

  // execute partition
  def partitionGraph(): RDD[String] = {
    // partition the data
    val partitionedData = nTriplesRDD
      .distinct
      .filter(line => line.getSubject.getURI.nonEmpty) // ignore SUBJECT with empty URI
      .map(line => {
          // SUBJECT, PREDICATE and OBJECT
          val getSubject = line.getSubject
          val getPredicate = line.getPredicate
          val getObject = line.getObject

          var filteredPredicate: Any = getPredicate
          var filteredObject: Any = getObject

          // set: PREDICATE
          if (getPredicate.isURI && getPredicate.getURI.contains(this.symbol("hash"))) {
              filteredPredicate = getPredicate.getURI.split(this.symbol("hash"))(1)

              // set: OBJECT where PREDICATE is a "type"
              if (filteredPredicate.equals("type") && getObject.isURI && getObject.getURI.contains(this.symbol("hash")))
                  filteredObject = this.symbol("colon") + getObject.getURI.split(this.symbol("hash"))(1)
              else if (getObject.isURI)
                  filteredObject = this.symbol("less-than") + getObject + this.symbol("greater-than")
              else
                  filteredObject = getObject

              // add colon at the start
              filteredPredicate = this.symbol("colon") + filteredPredicate
          } else {
              // PREDICATE
              if (getPredicate.isURI)
                  filteredPredicate = this.symbol("less-than") + getPredicate + this.symbol("greater-than")

              // OBJECT
              if (getObject.isURI)
                  filteredObject = this.symbol("less-than") + getObject + this.symbol("greater-than")
          }

          // (K,V) pair
          (
              this.symbol("less-than") + getSubject + this.symbol("greater-than"),
              filteredPredicate + this.symbol("space") + filteredObject + this.symbol("space")
          )
      })
      .reduceByKey(_ + _) // group based on key
      .sortBy(x => x._1) // sort by key
      .map(x => x._1 + this.symbol("space") + x._2) // output format

    partitionedData
  }

  // total partition time
  def partitionTime(processedTime: Long): Unit = {
    val milliseconds = TimeUnit.MILLISECONDS.convert(processedTime, TimeUnit.NANOSECONDS)
    val seconds = TimeUnit.SECONDS.convert(processedTime, TimeUnit.NANOSECONDS)
    val minutes = TimeUnit.MINUTES.convert(processedTime, TimeUnit.NANOSECONDS)

    if (milliseconds >= 0) {
      println("Processed Time (MILLISECONDS): " + milliseconds)

      if (seconds > 0) {
        println("Processed Time (SECONDS): " + seconds)

        if (minutes > 0) {
          println("Processed Time (MINUTES): " + minutes)
        }
      }
    }
  }
}
object RdfPartition {
  def apply(
    symbol:              Map[String, String],
    nTriplesRDD:         RDD[Triple],
    partitionedDataPath: String,
    numOfFilesPartition: Int) = new RdfPartition(symbol, nTriplesRDD, partitionedDataPath, numOfFilesPartition)
}
