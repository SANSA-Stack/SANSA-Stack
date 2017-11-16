
// packages
package net.sansa_stack.semantic_partitioning

// imports
import java.util.concurrent.TimeUnit
import org.apache.jena.graph.Triple
import org.apache.spark.rdd._

class DataPartition(
                     symbol: Map[String, String],
                     nTriplesRDD: RDD[Triple],
                     partitionedDataPath: String,
                     numOfFilesPartition: Int
                   ) extends Serializable {
  var _partitionData: RDD[String] = _

  // main
  def main(): Unit = {
    // start process time
    val startTime = System.nanoTime()

    // execute partition
    val partitionedData = this.executePartition()

    // end process time
    this.partitionTime(System.nanoTime() - startTime)

    // save data to output file
    if(partitionedData.partitions.nonEmpty) {
      partitionedData.repartition(this.numOfFilesPartition).saveAsTextFile(this.partitionedDataPath)
    }
  }

  // execute partition
  def executePartition(): RDD[String] = {
    // partition the data
    val partitionedData = nTriplesRDD
      .distinct
      .filter(
        line => {
          // ignore SUBJECT with empty URI
          line.getSubject.getURI.nonEmpty
        }
      )
      .map(line => {
        // SUBJECT, PREDICATE and OBJECT
        val getSubject    = line.getSubject
        val getPredicate  = line.getPredicate
        val getObject     = line.getObject

        var filteredPredicate: Any = getPredicate
        var filteredObject: Any = ()

        // filter out PREDICATE
        if(getPredicate.isURI && getPredicate.getURI.contains(this.symbol("hash"))) {
          filteredPredicate = getPredicate.getURI.split(this.symbol("hash"))(1)

          // filter out OBJECT where PREDICATE is a "type"
          if(filteredPredicate.equals("type") && getObject.isURI && getObject.getURI.contains(this.symbol("hash"))) {
            filteredObject = this.symbol("colon") + getObject.getURI.split(this.symbol("hash"))(1)
          } else if(!getObject.isURI) {
            filteredObject = getObject
          } else {
            filteredObject = this.symbol("less-than") + getObject + this.symbol("greater-than")
          }
        }

        // (K,V) pair
        (
          this.symbol("less-than") + getSubject + this.symbol("greater-than"),
          this.symbol("colon") + filteredPredicate + this.symbol("space") + filteredObject + this.symbol("space")
        )
      })
      .reduceByKey(_ + _) // group based on key
      .sortBy(x => x._1) // sort by key
      .map(x => x._1 + this.symbol("space") + x._2) // output format

    // assign data
    _partitionData = partitionedData

    partitionedData
  }

  // total partition time
  def partitionTime(processedTime: Long): Unit = {
    val milliseconds  = TimeUnit.MILLISECONDS.convert(processedTime, TimeUnit.NANOSECONDS)
    val seconds       = TimeUnit.SECONDS.convert(processedTime, TimeUnit.NANOSECONDS)
    val minutes       = TimeUnit.MINUTES.convert(processedTime, TimeUnit.NANOSECONDS)

    if(milliseconds >= 0) {
      println("Processed Time (MILLISECONDS): " + milliseconds)

      if(seconds > 0) {
        println("Processed Time (SECONDS): " + seconds)

        if(minutes > 0) {
          println("Processed Time (MINUTES): " + minutes)
        }
      }
    }
  }
}
