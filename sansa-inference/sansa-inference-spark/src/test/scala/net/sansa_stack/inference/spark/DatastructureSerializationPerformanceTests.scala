package net.sansa_stack.inference.spark

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}

import net.sansa_stack.inference.utils.{NTriplesStringToJenaTriple, NTriplesStringToRDFTriple}
import net.sansa_stack.rdf.spark.io.NTripleReader
// import org.apache.spark.groupon.metrics.{SparkMeter, SparkTimer, UserMetricsSystem}

import scala.reflect.ClassTag

/**
  * @author Lorenz Buehmann
  */
object DatastructureSerializationPerformanceTests {

  import org.apache.log4j.LogManager
  val logger = LogManager.getLogger("DatastructureSerializationPerformanceTests")

  case class JenaTriple(s: Node, p: Node, o: Node) // extends Tuple3[Node, Node, Node](s, p, o)

  // alias for the type to convert to and from
  type JenaTripleEncoded = (Node, Node, Node)



  implicit def rowToTriple(row: Row): Triple = Triple.create(row.getAs[Node](0), row.getAs[Node](1), row.getAs[Node](2))

  val conf = new SparkConf()
  conf.registerKryoClasses(Array(classOf[org.apache.jena.graph.Triple], classOf[org.apache.jena.graph.Node]))
  conf.set("spark.extraListeners", "net.sansa_stack.inference.spark.utils.CustomSparkListener")

  val parallelism = 20

  class JobListener extends SparkListener  {
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      logger.warn(s"Job completed, runTime: ${jobEnd.time}")
    }
  }

  // the SPARK config
  val session = SparkSession.builder
    .appName(s"RDF Triple Encoder Performance")
    .master("local[4]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.hadoop.validateOutputSpecs", "false") // override output files
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.default.parallelism", parallelism)
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.shuffle.partitions", parallelism)
    .config("spark.local.dir", "/home/user/spark/cache")
    .config(conf)
    .getOrCreate()

  session.sparkContext.addSparkListener(new JobListener)
  session.sparkContext.setCheckpointDir("/home/user/spark/checkpoint")

//  UserMetricsSystem.initialize(session.sparkContext, "SANSA-Namespace")
//
//  lazy val meter: SparkMeter = UserMetricsSystem.meter("MyMeter")
//  lazy val timer: SparkTimer = UserMetricsSystem.timer("MyTimer")

  def newSession(name: String): SparkSession =
    SparkSession.builder
    .appName(s"SPARK RDFS Reasoning")
    .master("local[4]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.hadoop.validateOutputSpecs", "false") // override output files
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.default.parallelism", parallelism)
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.shuffle.partitions", parallelism)
    .config(conf)
    .getOrCreate()


  def loadAndDistinctJena(path: String): Unit = {
    val triples = NTripleReader.load(session, path)

    triples.cache()

    // DISTINCT and COUNT
    val distinctCount = triples.distinct().count()

    // self JOIN on subject and COUNT
    val pair = triples.map(t => (t.getSubject, (t.getPredicate, t.getObject))) // map to PairRDD
    val joinCount = pair.join(pair).count()

    logger.info("Jena RDD[Triple]")
    logger.info(s"#triples:$distinctCount")
    logger.info(s"#joined triples(s-s):$joinCount")
  }

  def loadAndDistinctPlain(path: String): Unit = {
    val triples = session.sparkContext
      .textFile(path, 4) // read the text file
      .flatMap(line => new NTriplesStringToRDFTriple().apply(line))

    triples.cache()

    // DISTINCT and COUNT
    val distinctCount = triples.distinct().count()

    // self JOIN on subject and COUNT
    val pair = triples.map(t => (t.s, (t.p, t.o))) // map to PairRDD
    val joinCount = pair.join(pair).count()

    logger.info(distinctCount)
    logger.info(joinCount)
  }

  def loadAndDistinctDatasetJena(path: String): Unit = {
    import session.implicits._

    //    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[JenaTriple]

    // implicit conversions
    implicit def toEncoded(t: Triple): JenaTripleEncoded = (t.getSubject, t.getPredicate, t.getObject)
    implicit def fromEncoded(e: JenaTripleEncoded): Triple = Triple.create(e._1, e._2, e._3)

    implicit def single[A](implicit c: ClassTag[A]): Encoder[A] = Encoders.kryo[A](c)

    implicit def tuple3[A1, A2, A3](implicit e1: Encoder[A1], e2: Encoder[A2], e3: Encoder[A3]): Encoder[(A1, A2, A3)] =
      Encoders.tuple[A1, A2, A3](e1, e2, e3)

    val triplesRDD = NTripleReader.load(session, path)

    val tripleNodesRDD = triplesRDD.map(t => (t.getSubject, t.getPredicate, t.getObject))

    val conv = new NTriplesStringToJenaTriple()
    var tripleDS =
//      session.read.text(path)
//      .map(row => {
//      val t = conv.apply(row.getString(0))
//      (t.getSubject, t.getPredicate, t.getObject)
//    })
        session.createDataset(tripleNodesRDD)
      .toDF("s", "p", "o")
      .as[JenaTripleEncoded]

    tripleDS.printSchema()
    tripleDS.cache()

    // show 10 triples
    tripleDS.show()

    // DISTINCT and COUNT
    val distinctCount = tripleDS.distinct().count()


    // self JOIN on subject and COUNT
    val triplesA = tripleDS.alias("A")
    val triplesB = tripleDS.alias("B")
    val triplesJoined = triplesA.joinWith(triplesB, $"A.s" === $"B.s")
    val joinCount = triplesJoined.count()

    logger.info("DataFrame[(Node, Node, Node)]")
    logger.info(s"#triples:$distinctCount")
    logger.info(s"#joined triples(s-s):$joinCount")
  }

  def main(args: Array[String]): Unit = {

    val path = args(0)
//
//    loadAndDistinctJena(path)
//
//    loadAndDistinctPlain(path)

    loadAndDistinctDatasetJena(path)


    session.close()
  }
}
