package net.sansa_stack.ml.spark.anomalydetection

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.ml.spark.outliers.anomalydetection._
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class AnomalyDetectionTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.io._

  val wikiList = List("wikiPageRevisionID,wikiPageID")

  val path = getClass.getResource("/AnomalyDetection/testSample.nt").getPath

  val objList = List(
    "http://www.w3.org/2001/XMLSchema#double",
    "http://www.w3.org/2001/XMLSchema#integer",
    "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
    "http://dbpedia.org/datatype/squareKilometre")

  val triplesType = List("http://dbpedia.org/ontology")

  val listSuperType = List(
    "http://dbpedia.org/ontology/Activity", "http://dbpedia.org/ontology/Organisation",
    "http://dbpedia.org/ontology/Agent", "http://dbpedia.org/ontology/SportsLeague",
    "http://dbpedia.org/ontology/Person", "http://dbpedia.org/ontology/Athlete",
    "http://dbpedia.org/ontology/Event", "http://dbpedia.org/ontology/Place",
    "http://dbpedia.org/ontology/PopulatedPlace", "http://dbpedia.org/ontology/Region",
    "http://dbpedia.org/ontology/Species", "http://dbpedia.org/ontology/Eukaryote",
    "http://dbpedia.org/ontology/Location")

  val hypernym = "http://purl.org/linguistics/gold/hypernym"

  test("performing anomaly detection using HashingTF method should result in size 36") {

 

    val triples = spark.rdf(Lang.NTRIPLES)(path)
    triples.repartition(125).persist

    var clusterOfSubject: RDD[(Set[(String, String, Object)])] = null

    val outDetection1 = new AnomalyWithHashingTF(triples, objList, triplesType, 0.45, listSuperType, spark, hypernym, 125)
    clusterOfSubject = outDetection1.run()

    val cnt = clusterOfSubject.count()
    assert(cnt == 36)

  }

  test("performing anomaly detection using CountVetcorizerModel method should result in size 15") {


    val triples = spark.rdf(Lang.NTRIPLES)(path)
    triples.repartition(125).persist

    var clusterOfSubject: RDD[(Set[(String, String, Object)])] = null

    val outDetection1 = new AnomalyDetectionWithCountVetcorizerModel(triples, objList, triplesType, 0.45, listSuperType, spark, hypernym, 125)
    clusterOfSubject = outDetection1.run()

    val cnt = clusterOfSubject.count()
    assert(cnt == 15)
  }

  test("performing anomaly detection using CrossJoin method should result in size 15") {

    val triples = spark.rdf(Lang.NTRIPLES)(path)
    triples.repartition(125).persist

    var clusterOfSubject: RDD[(Set[(String, String, Object)])] = null

    val outDetection1 = new AnomalWithDataframeCrossJoin(triples, objList, triplesType, 0.45, listSuperType, spark, hypernym, 125)
    clusterOfSubject = outDetection1.run()

    val cnt = clusterOfSubject.count()
    assert(cnt == 36)
  }

}
