package net.sansa.rdf.spark.analytics
import org.apache.spark.graphx._


class SparkPageRank(lines: Graph[VertexId, String], iters: Int) {

  def showWarning() {
    System.err.println(
      """INFO: This is implementation of PageRank on SKOS dataset as a example!
      """.stripMargin)
  }

  showWarning()

  val links = lines.vertices.distinct().groupByKey().cache()

  var ranks = links.mapValues(v => 1.0)



  for (i <- 1 to iters) {
    val contribs = links.join(ranks).values.flatMap {
      case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
    }
    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
  }

  val output = ranks.collect().sortBy(_._1) //collect()
  println(output.mkString("\n"))

}
object SparkPageRank {
  def apply(lines: Graph[VertexId, String], iters: Int) = new SparkPageRank(lines, iters)
}