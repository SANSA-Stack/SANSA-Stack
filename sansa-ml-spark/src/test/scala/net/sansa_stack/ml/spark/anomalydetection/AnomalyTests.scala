package net.sansa_stack.ml.spark.anomalydetection

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.ml.spark.outliers.anomalydetection.AnomalyWithHashingTF
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class AnomalyTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.io._

  test("performing anomaly detection using HashingTF method should result in size 36") {
   
    // predicated that are not interesting for evaluation
    val wikiList = List("wikiPageRevisionID,wikiPageID")

    val path = getClass.getResource("/AnomalyDetection/testSample.nt").getPath

    //N-Triples Reader
    val triples = spark.rdf(Lang.NTRIPLES)(path)
    triples.repartition(125).persist

    //filtering numeric literal having xsd type double,integer,nonNegativeInteger and squareKilometre
    val objList = List(
      "http://www.w3.org/2001/XMLSchema#double",
      "http://www.w3.org/2001/XMLSchema#integer",
      "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
      "http://dbpedia.org/datatype/squareKilometre")

    //helful for considering only Dbpedia type as their will be yago type,wikidata type also
    val triplesType = List("http://dbpedia.org/ontology")

    //some of the supertype which are present for most of the subject
    val listSuperType = List(
      "http://dbpedia.org/ontology/Activity", "http://dbpedia.org/ontology/Organisation",
      "http://dbpedia.org/ontology/Agent", "http://dbpedia.org/ontology/SportsLeague",
      "http://dbpedia.org/ontology/Person", "http://dbpedia.org/ontology/Athlete",
      "http://dbpedia.org/ontology/Event", "http://dbpedia.org/ontology/Place",
      "http://dbpedia.org/ontology/PopulatedPlace", "http://dbpedia.org/ontology/Region",
      "http://dbpedia.org/ontology/Species", "http://dbpedia.org/ontology/Eukaryote",
      "http://dbpedia.org/ontology/Location")

    //hypernym URI
    val hypernym = "http://purl.org/linguistics/gold/hypernym"

    var clusterOfSubject: RDD[(Set[(String, String, Object)])] = null

    val outDetection1 = new AnomalyWithHashingTF(triples, objList, triplesType, 0.45, listSuperType, spark, hypernym, 125)
    clusterOfSubject = outDetection1.run()

    val cnt = clusterOfSubject.count()
    assert(cnt == 36)

  }
}
