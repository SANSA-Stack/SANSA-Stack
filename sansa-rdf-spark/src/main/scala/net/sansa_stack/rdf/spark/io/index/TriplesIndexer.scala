package net.sansa_stack.rdf.spark.io.index

import scala.collection.mutable

import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap
import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * @author Lorenz Buehmann
 */
class TriplesIndexer {

  val inputCols = Array("s", "p", "o")
  val outputCols = Array("s1", "p1", "o1")

  def index(triples: DataFrame): DataFrame = {

    val model = prepare(triples)

    model.transform(triples)
  }

  def prepare(triples: DataFrame): TripleIndexModel = {
    val counts = triples.rdd.treeAggregate(new Aggregator)(_.add(_), _.merge(_)).distinctArray

    val labels = counts.map(_.toSeq.sortBy(-_._2).map(_._1).toArray)

    new TripleIndexModel(labels, inputCols, outputCols)
  }

}

private[index] class Aggregator extends Serializable {

  var initialized: Boolean = false
  var k: Int = _
  var distinctArray: Array[mutable.HashMap[String, Long]] = _

  private def init(k: Int): Unit = {
    this.k = k
    distinctArray = new Array[mutable.HashMap[String, Long]](k)
    (0 until k).foreach { x =>
      distinctArray(x) = new mutable.HashMap[String, Long]
    }
    initialized = true
  }

  def add(r: Row): this.type = {
    if (!initialized) {
      init(r.size)
    }
    (0 until k).foreach { x =>
      val current = r.getString(x)
      val count: Long = distinctArray(x).getOrElse(current, 0L)
      distinctArray(x).put(current, count + 1)
    }
    this
  }

  def merge(other: Aggregator): Aggregator = {
    (0 until k).foreach { x =>
      other.distinctArray(x).foreach {
        case (key, value) =>
          val count: Long = this.distinctArray(x).getOrElse(key, 0L)
          this.distinctArray(x).put(key, count + value)
      }
    }
    this
  }
}

class TripleIndexModel(
  val labels: Array[Array[String]],
  val inputCols: Array[String],
  val outputCols: Array[String]) extends Serializable {

  val k = inputCols.length

  private val labelToIndex: Array[Object2DoubleOpenHashMap[String]] = {
    val k = labels.length
    val mapArray = new Array[Object2DoubleOpenHashMap[String]](k)
    (0 until k).foreach { x =>
      val n = labels(x).length
      mapArray(x) = new Object2DoubleOpenHashMap[String](k)
      var i = 0
      while (i < n) {
        mapArray(x).put(labels(x)(i), i)
        i += 1
      }
    }
    mapArray
  }

  def transform(dataset: DataFrame): DataFrame = {
    val transformedDataset = (0 until k).foldLeft[DataFrame](dataset) {
      case (df, x) =>
        val indexer = udf { label: String =>
          labelToIndex(x)(label)
        }
        val outputCol = outputCols(x)
        val metadata = NominalAttribute.defaultAttr.withName(outputCol)
          .withValues(labels(x)).toMetadata()

        df
          .withColumn(outputCol, indexer(col(inputCols(x))).as(outputCol, metadata))
          .drop(inputCols(x))

    }

    transformedDataset
  }
}

object TriplesIndexer {

  def apply: TriplesIndexer = new TriplesIndexer()

}
