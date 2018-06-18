package net.sansa_stack.inference.spark

import net.sansa_stack.inference.utils.{NTriplesStringToJenaTriple, NTriplesStringToRDFTriple}
import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerStageCompleted}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
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

  val parallelism = 4

  class JobListener extends SparkListener  {
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      logger.warn(s"Job completed, runTime: ${jobEnd.time}")
    }
  }

  // the SPARK config
  val session = SparkSession.builder
    .appName(s"SPARK RDFS Reasoning")
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


  def loadAndDictinctJena(path: String): Unit = {
    val triples = session.sparkContext
      .textFile(path, 4) // read the text file
      .map(new NTriplesStringToJenaTriple())

    triples.cache()

    // DISTINCT and COUNT
    val distinctCount = triples.distinct().count()

    // self JOIN on subject and COUNT
    val pair = triples.map(t => (t.getSubject, (t.getPredicate, t.getObject))) // map to PairRDD
    val joinCount = pair.join(pair).count()

    logger.info(distinctCount)
    logger.info(joinCount)
  }

  def loadAndDictinctPlain(path: String): Unit = {
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

    val triples = session.sparkContext
      .textFile(path, 4) // read the text file
      .map(new NTriplesStringToJenaTriple())
      .map(t => (t.getSubject, t.getPredicate, t.getObject))

    val conv = new NTriplesStringToJenaTriple()
    var tripleDS =
//      session.read.text(path)
//      .map(row => {
//      val t = conv.apply(row.getString(0))
//      (t.getSubject, t.getPredicate, t.getObject)
//    })
        session.createDataset(triples)
      .toDF("s", "p", "o")
      .as[JenaTripleEncoded]

    tripleDS.cache()

    // DISTINCT and COUNT
    val distinctCount = tripleDS.distinct().count()

    // self JOIN on subject and COUNT
    val joinCount = tripleDS.alias("A").join(tripleDS.alias("B"), $"A.s" === $"B.s", "inner").count()

    logger.info(distinctCount)
    logger.info(joinCount)
  }

  def main(args: Array[String]): Unit = {

    val path = args(0)

    loadAndDictinctJena(path)

    loadAndDictinctPlain(path)

    loadAndDistinctDatasetJena(path)


    session.close()
  }
}
