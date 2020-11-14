package net.sansa_stack.rdf.spark.partition.characteristc_sets

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.serializer.SerializationContext
import org.apache.jena.sparql.util.FmtUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.collect_set
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class CharacteristicSet(properties: Set[Node]) {}

/**
 * @author Lorenz Buehmann
 */
object CharacteristicSets {

  def computeCharacteristicSets(triples: RDD[Triple]): RDD[CharacteristicSet] = {
    triples
      //      .map(t => (t.getSubject, t.getPredicate))
      //      .groupByKey()
      .map(t => (t.getSubject, CharacteristicSet(Set(t.getPredicate))))
      .reduceByKey((c1, c2) => CharacteristicSet(c1.properties ++ c2.properties))
      .map(_._2)
      .distinct()
  }

  def computeCharacteristicSetsWithEntities(triples: RDD[Triple]): RDD[(CharacteristicSet, Set[Node])] = {
    triples
      .map(t => (t.getSubject, CharacteristicSet(Set(t.getPredicate))))
      .reduceByKey((c1, c2) => CharacteristicSet(c1.properties ++ c2.properties))
      .map(e => (e._2, Set(e._1)))
      .reduceByKey((nodes1, nodes2) => nodes1 ++ nodes2)
  }

  import scala.reflect.runtime.{universe => ru}

  private def getType[T](clazz: Class[T])(implicit runtimeMirror: ru.Mirror) = runtimeMirror.classSymbol(clazz).toType

  implicit val mirror = ru.runtimeMirror(getClass.getClassLoader)

  private def asRow(subj: Node, predObjPairs: Iterable[(Node, Node)]): Row = {
    // the subject first
    var values: IndexedSeq[AnyRef] = IndexedSeq[AnyRef](getObject(subj))

    var schema = new StructType()
    schema = schema.add("s", ScalaReflection.schemaFor(getType(getObject(subj).getClass)).dataType, false)

    // predicate-object pairs group by the predicate to generate a single column only
    // in case of multiple values
    predObjPairs.groupBy(_._1).mapValues(_.map(_._2)).foreach {
      case (prop, objects) =>
        if (objects.size == 1) {
          values = values :+ getObject(objects.head)
        } else {
          values = values :+ objects.map(getObject).toArray
        }
        schema = schema.add("P" + FmtUtils.stringForNode(prop), ScalaReflection.schemaFor(getType(getObject(objects.head).getClass)).dataType, false)
    }

    println(schema)

    Row.fromSeq(values)
  }

  private def getObject(node: Node): AnyRef = {
    if (node.isLiteral) {
      node.getLiteral.getValue
    } else {
      FmtUtils.stringForNode(node, null.asInstanceOf[SerializationContext])
    }
  }

  def computeCharacteristicSetsWithEntities(triples: DataFrame): DataFrame = {
    //    triples
    //      .select("s")
    //      .map(t => (t.getSubject, CharacteristicSet(Set(t.getPredicate))))
    //      .reduceByKey((c1, c2) => CharacteristicSet(c1.properties ++ c2.properties))
    //      .map(e => (e._2, Set(e._1)))
    //      .reduceByKey((nodes1, nodes2) => nodes1 ++ nodes2)
    triples
      .select("s", "p")
      .groupBy("s").agg(collect_set("p") as "cs")
      .groupBy("cs").agg(collect_set("s") as "entities")
  }


  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      throw new RuntimeException("Missing file path as argument")
    }

    val spark = SparkSession.builder
      .appName("Characteristic Sets computation")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import net.sansa_stack.rdf.spark.io._
    val path = args(0)
    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val css = computeCharacteristicSets(triples)

    css.collect().foreach(println)



    css.collect().foreach { cs =>

      val subjToPredObjPairs = triples
        .filter(t => cs.properties.contains(t.getPredicate))
        .map(t => (t.getSubject, (t.getPredicate, t.getObject)))
        .groupByKey()

      val rows = subjToPredObjPairs.map { case (s, po) => asRow(s, po) }
      rows.count()

//      val hf = Hashing.sha256().newHasher()
//      val tableName = hf.putString(cs.properties.toSeq.sortBy(_.getURI).mkString(","), Charsets.UTF_8).hash().toString

      //      val df = spark.createDataFrame(subjToPredObjPairs).createOrReplaceTempView(tableName)

    }

    spark.stop()
  }
}
