package net.sansa_stack.ml.spark.outliers.anomalydetection

import net.sansa_stack.rdf.spark.io.NTripleReader
import java.net.{ URI => JavaURI }
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    println("==================================================")
    println("|        Distributed Anomaly Detection           |")
    println("==================================================")

    //input file path
    if (args.length < 1) {
      System.err.println("No input file(s) found.")
      System.exit(1)
    }

    val input = args(0) //src/main/resources/anomaly/clusteringOntype.nt

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Anomaly Detection")
      .getOrCreate()

    //N-Triples Reader
    val triplesRDD = NTripleReader.load(sparkSession, JavaURI.create(input))

    //constant parameters defined
    val JSimThreshold = 0.6

    val objList = List("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
      "http://www.w3.org/2001/XMLSchema#date")

    //clustering of subjects are on the basis of rdf:type specially object with wikidata and dbpedia.org
    //val triplesType = List("http://www.wikidata.org", "http://dbpedia.org/ontology")
    val triplesType = List("http://dbpedia.org/ontology")
   
    //some of the supertype which are present for most of the subject
    val listSuperType=List(
                           "http://dbpedia.org/ontology/Activity","http://dbpedia.org/ontology/Organisation",
                           "http://dbpedia.org/ontology/Agent","http://dbpedia.org/ontology/SportsLeague",
                           "http://dbpedia.org/ontology/Person","http://dbpedia.org/ontology/Athlete",
                           "http://dbpedia.org/ontology/Event","http://dbpedia.org/ontology/Place",
                           "http://dbpedia.org/ontology/PopulatedPlace","http://dbpedia.org/ontology/Region",
                           "http://dbpedia.org/ontology/Species","http://dbpedia.org/ontology/Eukaryote",
                           "http://dbpedia.org/ontology/Location"
                          )
    //hypernym URI                      
    val hypernym="http://purl.org/linguistics/gold/hypernym"
    
    val outDetection = new AnomalyDetection(triplesRDD, objList, triplesType, JSimThreshold,listSuperType,sparkSession,hypernym)

    val clusterOfSubject = outDetection.run()

    clusterOfSubject.foreach(println)
    
    val setData=clusterOfSubject.map(f=>f._2)
    
    val listofData = clusterOfSubject.map({
      case (a, (b)) => b.map(f => (f._3.toString().toDouble)).toList
    })

    val listofDataArray = listofData.collect()

    for (listofData <- listofDataArray)
      IQR.iqr(listofData,setData)

  }
}
