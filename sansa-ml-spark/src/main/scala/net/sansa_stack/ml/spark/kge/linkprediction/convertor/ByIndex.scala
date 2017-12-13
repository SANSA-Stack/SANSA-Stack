package net.sansa_stack.ml.spark.kge.linkprediction.convertor

/**
 * ByIndex Class
 * -------------
 *
 * Transform Dataset[StringTriples] into Dataset[IntegerTriples] using index
 *  transformation
 *
 * Created by Hamed Shariat Yazdi
 */

import org.apache.spark.sql._

import net.sansa_stack.ml.spark.kge.linkprediction.triples.StringTriples
import net.sansa_stack.ml.spark.kge.linkprediction.triples.IntegerTriples

class ByIndex(data: Dataset[StringTriples], sk: SparkSession) extends Serializable with Convertor {

  val triples = numeric()

  def entities() = {
    data.select("Subject").union(data.select("Object")).distinct().collect()
  }

  def relations() = {
    data.select("Predicate").distinct().collect()
  }

  import sk.implicits._

  def numeric() = {
    data.map { i =>
      IntegerTriples(e.indexOf(Row(i.Subject)) + 1, r.indexOf(Row(i.Predicate)) + 1,
        e.indexOf(Row(i.Object)) + 1)
    }
  }

}