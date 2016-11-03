package org.dissect.rdf.spark

import scala.collection.JavaConversions._

import org.apache.commons.io.IOUtils
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
//import org.dissect.rdf.spark.io.JenaKryoRegistrator

object MainPartitioner {

  def partitionGraphByPredicates(graphRdd : RDD[Triple]) : Map[String, RDD[(Node, Node)]]= {
    val predicates = graphRdd.map(_.getPredicate).distinct().collect()

    val map = Map(predicates map { p => (
          p.getURI,
          graphRdd
            .filter(_.getPredicate.equals(p))
            .map(t => t.getSubject -> t.getObject)
            .persist())
          } : _*)
    map
  }

  def main(args: Array[String]): Unit = {
    val sparkContext = {
      val conf = new SparkConf().setAppName("BDE-readRDF").setMaster("local[1]")
        .set("spark.kryo.registrationRequired", "true") // use this for debugging and keeping track of which objects are being serialized.
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "org.dissect.rdf.spark.io.JenaKryoRegistrator")

      new SparkContext(conf)
    }

    val sqlContext = new SQLContext(sparkContext)

    val triplesString =
      """<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://xmlns.com/foaf/0.1/givenName>	"Guy De" .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/influenced>	<http://dbpedia.org/resource/Tobias_Wolff> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/influenced>	<http://dbpedia.org/resource/Henry_James> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/deathPlace>	<http://dbpedia.org/resource/Passy> .
        |<http://dbpedia.org/resource/Charles_Dickens>	<http://xmlns.com/foaf/0.1/givenName>	"Charles"@en .
        |<http://dbpedia.org/resource/Charles_Dickens>	<http://dbpedia.org/ontology/deathPlace>	<http://dbpedia.org/resource/Gads_Hill_Place> .""".stripMargin


    val it = RDFDataMgr.createIteratorTriples(IOUtils.toInputStream(triplesString), Lang.NTRIPLES, "http://example.org/").toSeq
    val graphRdd = sparkContext.parallelize(it)


    // graphRdd.mapPartitionsWithIndex(f, preservesPartitioning)

    val map = partitionGraphByPredicates(graphRdd)

    map.foreach(x => println(x._1, x._2.count))



    //println(predicates.mkString("\n"))

    sparkContext.stop()
  }
}