package net.sansa_stack.rdf.spark.partition.characteristc_sets

import java.nio.charset.Charset
import java.util.Comparator

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.util.NodeComparator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.collect_set
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.math.Ordering.comparatorToOrdering

import com.google.common.base.Charsets
import com.google.common.hash.Hashing


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

    val hf = Hashing.sha256().newHasher()

    css.foreach{cs =>

      val subjToPredObjPairs = triples
        .filter(t => cs.properties.contains(t.getPredicate))
        .map(t => (t.getSubject, (t.getPredicate, t.getObject)))
        .groupByKey()
        .map{case (s, po) => asRow(s, po)}


      val tableName = hf.putString(cs.properties.toSeq.sortBy(_.getURI).mkString(","), Charsets.UTF_8).hash().toString

//      val df = spark.createDataFrame(subjToPredObjPairs).createOrReplaceTempView(tableName)

    }

    def asRow(subj: Node, predObjPairs: Iterable[(Node, Node)]): Row = {
      val values = Seq()
      subj match {
        case n if n.isURI => n.getURI
        case n if n.isLiteral => n.getLiteral.getValue
      }
      Row.fromSeq(values)
    }

    spark.stop()

  }

}
