package net.sansa_stack.ml.spark.outliers.anomalydetection

import _root_.net.sansa_stack.rdf.spark.io.NTripleReader
import java.net.{ URI => JavaURI }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import scopt.OptionParser
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.log4j.Logger
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.io.IOException
import java.util.concurrent.TimeUnit
import org.apache.jena.riot.Lang
import org.apache.spark.storage.StorageLevel

object Main {
  @transient lazy val consoleLog: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    println("==================================================")
    println("|        Distributed Anomaly Detection           |")
    println("==================================================")

    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.threshold, config.anomalyListLimit, config.numofpartition, config.out, config.optionChange)
      case None =>
        consoleLog.warn(parser.usage)
    }
  }

  case class Config(in: String = "", threshold: Double = 0.0, anomalyListLimit: Int = 0, numofpartition: Int = 0, out: String = "", optionChange: Int = 0)

  val parser: OptionParser[Config] = new scopt.OptionParser[Config]("SANSA -Outlier Detection") {
    head("Detecting Numerical Outliers in dataset")

    //input file path
    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains RDF data (in N-Triples format)")

    //Jaccard similarity threshold value
    opt[Double]('t', "threshold").required().
      action((x, c) => c.copy(threshold = x)).
      text("the Jaccard Similarity value")

    //number of partition
    opt[Int]('a', "numofpartition").required().
      action((x, c) => c.copy(numofpartition = x)).
      text("Number of partition")
    
     //List limit for calculating IQR
    opt[Int]('c', "anomalyListLimit").required().
      action((x, c) => c.copy(anomalyListLimit = x)).
      text("the outlier List Limit")

    //output file path
    opt[String]('o', "output").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    //option for changing different class
    opt[Int]('z', "optionChange").required().
      action((x, c) => c.copy(optionChange = x)).
      text("Option Number for class")

  }
  // remove path files
  def removePathFiles(root: Path): Unit = {
    if (Files.exists(root)) {
      Files.walkFileTree(root, new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      })
    }
  }

  def run(input: String, JSimThreshold: Double, anomalyListLimit: Int, numofpartition: Int, output: String, optionChange: Int): Unit = {

    removePathFiles(Paths.get(output))

    //spark configuration
    val sparkSession = SparkSession.builder
      .master("spark://172.18.160.16:3077")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "400")
      .config("spark.sql.autoBroadcastJoinThreshold", "304857600")
      .config("spark.executor.overhead.memory", "2048")
      .config("spark.driver.overhead.memory", "2048")
      .appName("Anomaly Detection")
      .getOrCreate()
  
    // predicated that are not interesting for evaluation
    val wikiList = List("wikiPageRevisionID,wikiPageID")

    //N-Triples Reader
    val triplesRDD = NTripleReader.load(sparkSession, JavaURI.create(input)).repartition(numofpartition)
    triplesRDD.persist()
  
    //filtering numeric literal having xsd type double,integer,nonNegativeInteger and squareKilometre
    val objList = List("http://www.w3.org/2001/XMLSchema#double",
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

    if (optionChange == 0) {

      println("AnomalyDetection-using ApproxSimilarityJoin function with the help of CountVectorizerModel")
      val outDetection = new AnomalyDetectionWithCountVetcorizerModel(triplesRDD, objList, triplesType, JSimThreshold, listSuperType, sparkSession, hypernym, numofpartition)
      val startTime = System.nanoTime()
      clusterOfSubject = outDetection.run()

      val setData = clusterOfSubject.repartition(1000).persist(StorageLevel.MEMORY_AND_DISK)
      val setDataStore = setData.map(f => f.toSeq)

      val setDataSize = setDataStore.filter(f => f.size > anomalyListLimit)
      val test = setDataSize.map(f => outDetection.iqr2(f, anomalyListLimit))
      val testfilter = test.filter(f => f.size > 0)
      val testfilterDistinct = testfilter.flatMap(f => f)
      testfilterDistinct.saveAsTextFile(output)
      setData.unpersist()
      runTime(System.nanoTime() - startTime)

    } else if (optionChange == 1) {

      println("AnomalyDetection-using ApproxSimilarityJoin function with the help of HashingTF ")
      val outDetection1 = new AnomalyWithHashingTF(triplesRDD, objList, triplesType, JSimThreshold, listSuperType, sparkSession, hypernym, numofpartition)
      val startTime = System.nanoTime()
      clusterOfSubject = outDetection1.run()

      val setData = clusterOfSubject.repartition(1000).persist(StorageLevel.MEMORY_AND_DISK)
      val setDataStore = setData.map(f => f.toSeq)

      val setDataSize = setDataStore.filter(f => f.size > anomalyListLimit)

      val test = setDataSize.map(f => outDetection1.iqr2(f, anomalyListLimit))
      
      
      val testfilter = test.filter(f => f.size > 0) //.distinct()
       val testfilterDistinct = testfilter.flatMap(f => f)
      testfilterDistinct.saveAsTextFile(output)
      setData.unpersist()
      runTime(System.nanoTime() - startTime)

    }  else if (optionChange == 2) {
      println("Cartesian product using dataframe crossJoin")
      val outDetection2 = new AnomalWithDataframeCrossJoin(triplesRDD, objList, triplesType, JSimThreshold, listSuperType, sparkSession, hypernym, numofpartition)
      val startTime = System.nanoTime()
      clusterOfSubject = outDetection2.run()
      val setData = clusterOfSubject.repartition(1000).persist(StorageLevel.MEMORY_AND_DISK)
      val setDataStore = setData.map(f => f.toSeq)

      val setDataSize = setDataStore.filter(f => f.size > anomalyListLimit)
      val test = setDataSize.map(f => outDetection2.iqr2(f, anomalyListLimit))
      val testfilter = test.filter(f => f.size > 0)
      val testfilterDistinct = testfilter.flatMap(f => f)
      testfilterDistinct.saveAsTextFile(output)
      setData.unpersist()
      runTime(System.nanoTime() - startTime)

    }

    sparkSession.stop()

  }

  def runTime(processedTime: Long): Unit = {
    val milliseconds = TimeUnit.MILLISECONDS.convert(processedTime, TimeUnit.NANOSECONDS)
    val seconds = Math.floor(milliseconds / 1000d + .5d).toInt
    val minutes = TimeUnit.MINUTES.convert(processedTime, TimeUnit.NANOSECONDS)

    if (milliseconds >= 0) {
      consoleLog.info(s"Processed Time (MILLISECONDS): $milliseconds")

      if (seconds > 0) {
        consoleLog.info(s"Processed Time (SECONDS): $seconds approx.")

        if (minutes > 0) {
          consoleLog.info(s"Processed Time (MINUTES): $minutes")
        }
      }
    }
  }
}
