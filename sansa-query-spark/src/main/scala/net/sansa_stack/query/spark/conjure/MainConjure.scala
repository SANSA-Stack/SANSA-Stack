package net.sansa_stack.query.spark.conjure

import java.io.{ByteArrayOutputStream, File}
import java.net.{BindException, InetAddress, URL}
import java.util.concurrent.TimeUnit

import com.google.common.base.StandardSystemProperty
import com.google.common.collect.{ImmutableRangeSet, Range, RangeSet}
import org.aksw.jena_sparql_api.ext.virtuoso.HealthcheckRunner
import org.apache.jena.fuseki.FusekiException
import org.apache.jena.fuseki.main.FusekiServer
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionRemote}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.jena.vocabulary.RDF
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession


object MainConjure {

  /*
  def startSparqlEndpoint(): Server = {
    var result = ""
    try {
      result = startSparqlEndpointCore
    }
    catch {
      case _: Throwable => result = "failed"
    }

    result
  }
  */

  def startSparqlEndpoint(): (FusekiServer, URL) = {

    var result: (FusekiServer, URL) = null

    var port = 3030
    val name = "test"

    // If the server is already up, we are done
    var url: URL = null
    //    try {
    //      HealthcheckRunner.checkUrl(url)
    //    }
    //    catch {
    //      case _: Throwable =>

    var retry = 10
    while (retry > 0) {
      url = HealthcheckRunner.createUrl("http://localhost:" + port + "/" + name)
      val ds = DatasetFactory.createTxnMem

      ds.getDefaultModel.add(RDF.`type`, RDF.`type`, RDF.`type`)

      val server = FusekiServer.create()
        .add(name, ds)
        .port(port)
        .build

      try {
        println(TaskContext.getPartitionId() + " Attempting to start: " + url)
        println(TaskContext.getPartitionId() + " " + retry + " retries remaining")
        server.start();

        result = (server, url)
        retry = 0
      }
      catch {
        case e: FusekiException => e.getCause match {
          case f: BindException =>
            server.stop
            println(TaskContext.getPartitionId() + " BIND EXCEPTION")
            port = port + 1
            retry = retry - 1 /* already running */
            if (retry <= 0) throw new RuntimeException("Giving up")
          case e => throw new RuntimeException(e)
        }
      }
    }

    // println(TaskContext.getPartitionId() + "Creating URL...")
    val str = url.toString + "?query=SELECT%20*%20{%20%3Curn:s%3E%20%3Curn:p%3E%20%20?o%20}%20LIMIT%20%201"
    // println(TaskContext.getPartitionId() + "Testing " + str)
    val checkUrl = HealthcheckRunner.createUrl(str)
    println(TaskContext.getPartitionId() + " Health check with " + checkUrl)
    new HealthcheckRunner(60, 1, TimeUnit.SECONDS, new Runnable {
      override def run(): Unit = HealthcheckRunner.checkUrl(checkUrl)
    })

    println(TaskContext.getPartitionId() + " Success!")
    return result
  }


  def main(args: Array[String]): Unit = {


    val tmpDirStr = StandardSystemProperty.JAVA_IO_TMPDIR.value()
    if (tmpDirStr == null) {
      throw new RuntimeException("Could not obtain temporary directory")
    }
    val sparkEventsDir = new File(tmpDirStr + "/spark-events")
    if (!sparkEventsDir.exists()) {
      sparkEventsDir.mkdirs()
    }

    // Lambda that maps host names to allowed port ranges (for the triple store)
    val hostToPortRanges: Function[String, RangeSet[Integer]] =
      hostName => new ImmutableRangeSet.Builder[Integer].add(Range.closed(3030, 3040)).build()

    // File.createTempFile("spark-events")

    val sparkSession = SparkSession.builder
      .master("local[8]")
      .appName("spark session example")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      .config("spark.default.parallelism", "4")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")

    val hostToPortRangesBroadcast: Broadcast[String => RangeSet[Integer]] =
      sparkSession.sparkContext.broadcast(hostToPortRanges)


    val it = Seq.range(0, 1000)
      .map(i => s"CONSTRUCT WHERE { ?s$i ?p ?o }")

    val rdd = sparkSession.sparkContext.parallelize(it)


    // TODO Maybe accumulate on the workers if distinct doesn't do that already
    val workerHostNames = rdd
      .mapPartitions(_ => Iterator(InetAddress.getLocalHost.getHostName))
      .distinct.collect.toSet


    println("Hostnames: " + workerHostNames)

    val statusReports = rdd
      .mapPartitions(it => mapWithConnection(hostToPortRangesBroadcast)(it)((item, conn) => {
        println(TaskContext.getPartitionId() + " processing " + item)
        val model = conn.queryConstruct(item)
        val baos = new ByteArrayOutputStream
        RDFDataMgr.write(baos, model, RDFFormat.TURTLE_PRETTY)
        val str = baos.toString("UTF-8")
        (item, str)
      }))
      .collect

    println("RESULTS: ----------------------------")
    for (item <- statusReports) {
      // println(item)
    }


    sparkSession.stop
    sparkSession.close()


    println("Done")
  }


  //  def wrapperFactory[T, X]

  //def mapWithConnectionFactory[T, X](hostToPortRangesBroadcast: Broadcast[String => RangeSet[Integer]]): (Iterator[T], (T, RDFConnection) => X) => Iterator[X] =
 //   (it: Iterator[T], fn: (T, RDFConnection) => X) => mapWithConnection[T, X](hostToPortRangesBroadcast, it, fn)

  def mapWithConnection[T, X](hostToPortRangesBroadcast: Broadcast[String => RangeSet[Integer]])
                             (it: Iterator[T])
                             (fn: (T, RDFConnection) => X): Iterator[X] = {
    val hostName = InetAddress.getLocalHost.getHostName

    val hostToPortRanges = hostToPortRangesBroadcast.value
    val portRanges = hostToPortRanges(hostName)
    println("Port ranges: " + portRanges)

    val (server, url) = startSparqlEndpoint()

    println(TaskContext.getPartitionId()  + " Got endpoint at " + url)

    val conn = RDFConnectionRemote.create()
      .destination(url.toString)
      .build()

    val onClose = () => {
      println(TaskContext.getPartitionId() + " stopping server")
      server.stop()
      conn.close()
    }

    // Construct an iterator that releases resources upon encountering the last item
    // Temporarily pairs each item with an 'isLastItem' flag
    val wrapperIt: Iterator[T] =
      (it.map(x => (x, false)) ++ Iterator((null.asInstanceOf[T], true)))
      .filter(x => if (x._2 == true) { onClose(); false } else true)
      .map(x => x._1)

    // Free resources on exception
    wrapperIt.map(item => try {
      val s: X = fn.apply(item, conn)
      s
    } catch {
      case e => onClose; throw new RuntimeException(e)
    })
  }
}
