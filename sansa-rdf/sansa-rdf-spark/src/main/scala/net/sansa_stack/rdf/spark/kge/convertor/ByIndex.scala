package net.sansa_stack.rdf.spark.kge.convertor

/**
 * ByIndex Class
 * -------------
 *
 * Transform Dataset[StringTriples] into Dataset[IntegerTriples] using index
 * transformation
 *
 * Created by Hamed Shariat Yazdi
 */

import net.sansa_stack.rdf.spark.kge.triples.{ IntegerTriples, StringTriples }
import org.apache.spark.sql._

class ByIndex(data: Dataset[StringTriples], sk: SparkSession)
  extends Serializable with Convertor {

  val triples = numeric()

  def getEntities(): Array[Row] = {
    data.select("Subject").union(data.select("Object")).distinct().collect()
  }

  def getRelations(): Array[Row] = {
    data.select("Predicate").distinct().collect()
  }

  import sk.implicits._

  def numeric(): Dataset[IntegerTriples] = {
    data.map { i =>
      IntegerTriples(e.indexOf(Row(i.Subject)) + 1, r.indexOf(Row(i.Predicate)) + 1,
        e.indexOf(Row(i.Object)) + 1)
    }
  }

}
