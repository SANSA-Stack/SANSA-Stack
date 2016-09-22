package net.sansa.rdf.spark

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.SparkConf
import net.sansa.rdf.spark.model.JenaSparkRDDOps
import net.sansa.rdf.spark.model.TripleRDD._

object App {

  def main(args: Array[String]): Unit = {
    val sparkContext = {
      val conf = new SparkConf().setAppName("BDE-readRDF").setMaster("local[1]")
//        .set("spark.kryo.registrationRequired", "true") // use this for debugging and keeping track of which objects are being serialized.
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "org.dissect.rdf.spark.io.JenaKryoRegistrator")

      new SparkContext(conf)
    }

    val triplesString =
      """<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://xmlns.com/foaf/0.1/givenName>	"Guy De" .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/influenced>	<http://dbpedia.org/resource/Tobias_Wolff> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/influenced>	<http://dbpedia.org/resource/Henry_James> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/deathPlace>	<http://dbpedia.org/resource/Passy> .
        |<http://dbpedia.org/resource/Charles_Dickens>	<http://xmlns.com/foaf/0.1/givenName>	"Charles"@en .
        |<http://dbpedia.org/resource/Charles_Dickens>	<http://dbpedia.org/ontology/deathPlace>	<http://dbpedia.org/resource/Gads_Hill_Place> .""".stripMargin

    val ops = JenaSparkRDDOps(sparkContext)
    import ops._

    val triples = fromNTriples(triplesString, "http://dbpedia.org").toSeq
    println(triples.mkString("\n"))

    val graph = sparkContext.parallelize(triples)
    println("All objects for predicate influenced:\n" + graph.getObjectsWithPredicate(URI("http://dbpedia.org/ontology/influenced")).collect().mkString("\n"))
    println("All triples related to Dickens:\n" + graph.find(URI("http://dbpedia.org/resource/Charles_Dickens"), ANY, ANY).collect().mkString("\n"))
    println("All triples with predicate deathPlace:\n" + graph.find(ANY, URI("http://dbpedia.org/ontology/deathPlace"), ANY).collect().mkString("\n"))

    sparkContext.stop()
  }
}

