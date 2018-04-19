package net.sansa_stack.ml.spark.outliers.anomalydetection

import net.sansa_stack.rdf.spark.io.NTripleReader
import java.net.{ URI => JavaURI }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import scopt.OptionParser
import org.apache.log4j.Logger
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.io.IOException
import java.util.concurrent.TimeUnit
object Main {
  @transient lazy val consoleLog: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    println("==================================================")
    println("|        Distributed Anomaly Detection           |")
    println("==================================================")

    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.threshold, config.anomalyListLimit, config.numofpartition, config.out)
      case None =>
        consoleLog.warn(parser.usage)
    }
  }

  case class Config(in: String = "", threshold: Double = 0.0, anomalyListLimit: Int = 0, numofpartition: Int = 0, out: String = "")

  val parser: OptionParser[Config] = new scopt.OptionParser[Config]("SANSA -Outlier Detection") {
    head("Detecting Numerical Outliers in dataset")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains RDF data (in N-Triples format)")

    opt[Double]('t', "threshold").required().
      action((x, c) => c.copy(threshold = x)).
      text("the Jaccard Similarity value")

    opt[Int]('a', "numofpartition").required().
      action((x, c) => c.copy(numofpartition = x)).
      text("Number of partition")

    opt[Int]('c', "anomalyListLimit").required().
      action((x, c) => c.copy(anomalyListLimit = x)).
      text("the outlier List Limit")

    opt[String]('o', "output").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

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

  def run(input: String, JSimThreshold: Double, anomalyListLimit: Int, numofpartition: Int, output: String): Unit = {
    removePathFiles(Paths.get(output))
    val sparkSession = SparkSession.builder
    //  .master("spark://172.18.160.16:3077")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Anomaly Detection")
      .getOrCreate()
    println("JSimThreshold=" + JSimThreshold)
    println("anomalyListLimit=" + anomalyListLimit)
    println("input=" + input)
    println("number of partition" + numofpartition)
    
    //N-Triples Reader
    val triplesRDD = NTripleReader.load(sparkSession, JavaURI.create(input)).repartition(numofpartition).persist()

    val objList = List("http://www.w3.org/2001/XMLSchema#double",
      "http://www.w3.org/2001/XMLSchema#integer",
      "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
      "http://dbpedia.org/datatype/squareKilometre")

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

    val startTime = System.nanoTime()

    val outDetection = new AnomalyDetection(triplesRDD, objList, triplesType, JSimThreshold, listSuperType, sparkSession, hypernym, numofpartition)

    //run method will return the KV pair-Key as property and value as clustered numerical triples
    //example: population,Set(India,population,200000000,Germany,population,800000,........)
    val clusterOfSubject = outDetection.run()

    clusterOfSubject.take(10).foreach(println)
    
    val setData = clusterOfSubject.repartition(numofpartition).persist.map(f => f._2.toSeq)

    //calculating IQR and saving output to the file
    val listofDataArray = setData.collect()
    var a: Dataset[Row] = null

    for (listofDatavalue <- listofDataArray) {
      a = outDetection.iqr1(listofDatavalue, anomalyListLimit)
      if (a != null)
        a.select("dt").coalesce(1).write.format("text").mode(SaveMode.Append) save (output)
    }
    setData.unpersist()
    runTime(System.nanoTime() - startTime)
    // scala.io.StdIn.readLine()
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
