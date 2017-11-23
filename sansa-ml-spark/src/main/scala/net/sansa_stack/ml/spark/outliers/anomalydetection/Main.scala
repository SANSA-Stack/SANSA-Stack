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
    
    val input = args(0)                       //src/main/resources/anomaly/clusteringOntype.nt
    
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Anomaly Detection")
      .getOrCreate()

    //N-Triples Reader
    val triplesRDD = NTripleReader.load(sparkSession, JavaURI.create(input))
    
    //constant parameters defined
    val JSimThreshold=0.7
    
    val objList = List("http://www.w3.org/2001/XMLSchema#integer",
                       "http://www.w3.org/2001/XMLSchema#double",
                        "http://www.w3.org/2001/XMLSchema#nonNegativeInteger")
                        
    //clustering of subjects are on the basis of rdf:type specially object with wikidata and dbpedia.org
    val triplesType = List("http://www.wikidata.org", "http://dbpedia.org/ontology")

    val outDetection = new AnomalyDetection(triplesRDD, objList, triplesType,JSimThreshold)
    
    val clusterOfSubject=outDetection.run()
    
    clusterOfSubject.foreach(println)
    
    sparkSession.stop
  }

}
