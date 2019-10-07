package net.sansa_stack.query.spark.conjure

import java.io.{ByteArrayOutputStream, File}
import java.net.{BindException, InetAddress, URL}
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.aksw.dcat.ap.utils.DcatUtils
import org.aksw.jena_sparql_api.common.DefaultPrefixes
import org.aksw.jena_sparql_api.conjure.dataref.rdf.api.DataRefUrl
import org.aksw.jena_sparql_api.conjure.dataset.algebra._
import org.aksw.jena_sparql_api.conjure.dataset.engine.OpExecutorDefault
import org.aksw.jena_sparql_api.ext.virtuoso.HealthcheckRunner
import org.aksw.jena_sparql_api.http.repository.impl.HttpResourceRepositoryFromFileSystemImpl
import org.aksw.jena_sparql_api.mapper.proxy.JenaPluginUtils
import org.aksw.jena_sparql_api.rx.SparqlRx
import org.aksw.jena_sparql_api.stmt.SparqlStmtParserImpl
import org.aksw.jena_sparql_api.utils.Vars
import org.apache.jena.ext.com.google.common.base.{StandardSystemProperty, Stopwatch}
import org.apache.jena.ext.com.google.common.collect.{DiscreteDomain, ImmutableRangeSet, Range}
import org.apache.jena.ext.com.google.common.hash.Hashing
import org.apache.jena.fuseki.FusekiException
import org.apache.jena.fuseki.main.FusekiServer
import org.apache.jena.query.{DatasetFactory, Syntax}
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionFactory, RDFConnectionRemote}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.jena.sys.JenaSystem
import org.apache.jena.vocabulary.RDF
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.util.Random


object MainConjure extends LazyLogging {

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

  def startSparqlEndpoint(portRanges: ImmutableRangeSet[Integer]): (FusekiServer, URL) = {

    val sortedPorts = portRanges.asSet(DiscreteDomain.integers).asScala.toList
    val shuffledPorts = Random.shuffle(sortedPorts)

    // logger.info("Sorted ports: " + sortedPorts)

    val name = "test"
    var result: (FusekiServer, URL) = null

    var url: URL = null

    var it = shuffledPorts.iterator
    while(it.hasNext) {
      val port = it.next
      url = HealthcheckRunner.createUrl("http://localhost:" + port + "/" + name)
      val ds = DatasetFactory.createTxnMem

      ds.getDefaultModel.add(RDF.`type`, RDF.`type`, RDF.`type`)

      val server = FusekiServer.create()
        .add(name, ds)
        .port(port)
        .build

      try {
        logger.info(TaskContext.getPartitionId() + " Attempting to start: " + url)
        server.start();

        result = (server, url)
        it = Iterator() /* break out of the loop */
      }
      catch {
        case e: FusekiException => e.getCause match {
          case f: BindException =>
            server.stop
            logger.info(TaskContext.getPartitionId() + " BIND EXCEPTION")
            if (!it.hasNext) throw new RuntimeException("Tried all allowed ports - giving up")
          case e => throw new RuntimeException(e)
        }
      }
    }

    // logger.info(TaskContext.getPartitionId() + "Creating URL...")
    val str = url.toString + "?query=SELECT%20*%20{%20%3Curn:s%3E%20%3Curn:p%3E%20%20?o%20}%20LIMIT%20%201"
    // logger.info(TaskContext.getPartitionId() + "Testing " + str)
    val checkUrl = HealthcheckRunner.createUrl(str)
    logger.info(TaskContext.getPartitionId() + " Health check with " + checkUrl)
    new HealthcheckRunner(60, 1, TimeUnit.SECONDS, new Runnable {
      override def run(): Unit = HealthcheckRunner.checkUrl(checkUrl)
    })

    logger.info(TaskContext.getPartitionId() + " Success!")
    return result
  }

  def createPartitionKey(dcatDataset: Resource): String = {
    val key = Option(DcatUtils.getFirstDownloadUrl(dcatDataset)).getOrElse("")

    return key
  }

  def main(args: Array[String]): Unit = {

    val catalogUrl = if (args.length == 0) "http://localhost/~raven/conjure.test.dcat.ttl" else args(0)
    val limit = if (args.length > 1) args(1).toInt else 1000


    val tmpDirStr = StandardSystemProperty.JAVA_IO_TMPDIR.value()
    if (tmpDirStr == null) {
      throw new RuntimeException("Could not obtain temporary directory")
    }
    val sparkEventsDir = new File(tmpDirStr + "/spark-events")
    if (!sparkEventsDir.exists()) {
      sparkEventsDir.mkdirs()
    }

    // Lambda that maps host names to allowed port ranges (for the triple store)
    // Only ImmutableRangeSet provides the .asSet(discreteDomain) view
    val hostToPortRanges: String => ImmutableRangeSet[Integer] =
      hostName => new ImmutableRangeSet.Builder[Integer].add(Range.closed(3030, 3040)).build()

    // File.createTempFile("spark-events")
    val numThreads = 4
    val numPartitions = numThreads * 1

    val masterHostname = InetAddress.getLocalHost.getHostName;

    val builder = SparkSession.builder

    if (!masterHostname.toLowerCase.contains("qrowd")) {
      builder.master(s"local[$numThreads]")
    }

    val sparkSession = builder
      .appName("Sansa-Conjure Test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorRDFNode"))
      .config("spark.default.parallelism", s"$numThreads")
      .config("spark.sql.shuffle.partitions", s"$numThreads")
      .getOrCreate()

    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")

    val hostToPortRangesBroadcast: Broadcast[String => ImmutableRangeSet[Integer]] =
      sparkSession.sparkContext.broadcast(hostToPortRanges)

    // TODO Circular init issue with DefaultPrefixes
    // We could use ARQConstants.getGlobalPrefixMap()
    JenaSystem.init

    // Create a SPARQL parser with preconfigured prefixes
    // Pure luxury!
    val parser = SparqlStmtParserImpl.create(Syntax.syntaxARQ, DefaultPrefixes.prefixes, false)

    val dcatQuery = parser.apply(s"""
       CONSTRUCT {
        ?a ?b ?c .
        ?c ?d ?e
      } {

        { SELECT DISTINCT ?a {
          ?a dcat:distribution [
            dcat:byteSize ?byteSize
          ]
          FILTER(?byteSize < 100000)
        } LIMIT $limit }

        ?a ?b ?c
        OPTIONAL { ?c ?d ?e }
      }""").getAsQueryStmt.getQuery

//    val catalog = RDFDataMgr.loadModel("small.nt")
    val catalog = RDFDataMgr.loadModel(catalogUrl)
    val conn = RDFConnectionFactory.connect(DatasetFactory.create(catalog))

    val entries = SparqlRx.execConstructGrouped(conn, Vars.a, dcatQuery)
        .toList.blockingGet
        .asScala
        .map(_.asResource);

    // Prepare the data for distribution to the nodes

    // TODO Maybe accumulate on the workers if distinct doesn't do that already
    val dummyRdd = sparkSession.sparkContext.parallelize(Seq.range(0, 1000))

    val workerHostNames = dummyRdd
      .mapPartitions(_ => Iterator(InetAddress.getLocalHost.getHostName))
      .distinct.collect.sorted.toList


    logger.info("Hostnames: " + workerHostNames)

    val numHosts = workerHostNames.size
    // Create hash from the lexicographically lowest downloadURL

    // Use guava's hashing facilities to create a
    // non JVM-dependent hash (in contrast to Object.hashCode)
    val hashFunction = Hashing.goodFastHash(32)
    val hashInt: Resource => Int = res => math.abs(hashFunction.newHasher.putString(
      createPartitionKey(res), StandardCharsets.UTF_8).hash().asInt().toInt)

    // Combine the data with the location preferences
    val inputDataWithLocPrefs = entries.map(r => (r, Seq(workerHostNames(hashInt(r) % numHosts))))

    for (item <- inputDataWithLocPrefs) {
      logger.info("Item: " + item)
//      RDFDataMgr.write(System.out, item.getModel, RDFFormat.TURTLE_PRETTY)
    }

    // The RDD does not contain the location preferences anymore of course
    val dcatRdd = sparkSession.sparkContext
      .makeRDD(inputDataWithLocPrefs)
      .coalesce(numPartitions)

    /*
    for (item <- dcatRdd.collect) {
      logger.info(item)
      RDFDataMgr.write(System.out, item.getModel, RDFFormat.TURTLE_PRETTY)
    }
    */

    val v = OpVar.create("dataRef")
    val opWorkflow = OpConstruct.create(v, parser.apply(
      """CONSTRUCT {
           <env:datasetId>
             eg:predicateReport ?report ;
             .

           ?report
             eg:entry [
               eg:predicate ?p ;
               eg:numUses ?numTriples ;
               eg:numUniqS ?numUniqS ;
               eg:numUniqO ?numUniqO
             ]
           }
           {
             # TODO Allocate some URI based on the dataset id
             BIND(BNODE() AS ?report)
             { SELECT ?p (COUNT(*) AS ?numTriples) (COUNT(DISTINCT ?s) AS ?numUniqS) (COUNT(DISTINCT ?o) AS ?numUniqO) {
               ?s ?p ?o
             } GROUP BY ?p }
           }
      """).toString)


    // Note .asResource yields a Jena ResourceImpl instead of 'this'
    // so that kryo can serialize it
    // The .asResource behavior is subject to change. If it breaks, create an implicit
    // function such as Resource.toDefaultResource
    val workflowBroadcast: Broadcast[Resource] = sparkSession.sparkContext.broadcast(opWorkflow.asResource)

    logger.info("NUM PARTITIONS = " + dcatRdd.getNumPartitions)

    val executiveRdd = dcatRdd.mapPartitions(it => {

      classOf[JenaSystem].synchronized {
        JenaSystem.init()
        println("TypeDecider stuff: " + JenaPluginUtils.getTypeDecider())
      }

      val opPlainWorfklow = workflowBroadcast.value;
      println("RECEIVED CONJURE WORKFLOW:")
      RDFDataMgr.write(System.out, opPlainWorfklow.getModel, RDFFormat.TURTLE_PRETTY)
      val opWorkflow = JenaPluginUtils.polymorphicCast(opPlainWorfklow, classOf[Op])

      if(opWorkflow == null) {
        throw new RuntimeException("op of workflow was null, workflow itself was: " + opPlainWorfklow)
      }

      // Set up the repo on the worker
      // TODO Test for race conditions
      val repo = HttpResourceRepositoryFromFileSystemImpl.createDefault
      val executor = new OpExecutorDefault(repo)

      it.map(dcat => {
        val parser = SparqlStmtParserImpl.create(Syntax.syntaxARQ, DefaultPrefixes.prefixes, false)

        logger.info("Processing: " + dcat)

        val url = DcatUtils.getFirstDownloadUrl(dcat)
        logger.info("Download URL is: " + dcat)
        if(url != null) {
          val dataRef = DataRefUrl.create(url)

//          val dataRef = DataRefOp.create(OpUpdateRequest.create(OpData.create,
//            parser.apply("INSERT DATA { <urn:s> <urn:p> <urn:o> }").toString))

          val map = Collections.singletonMap("dataRef", OpDataRefResource.from(dataRef))
          val effectiveWorkflow = OpUtils.copyWithSubstitution(opWorkflow, map)

          val data = effectiveWorkflow.accept(executor)
          val conn = data.openConnection
          val model = conn.queryConstruct("CONSTRUCT WHERE { ?s ?p ?o }")
          // RDFDataMgr.write(System.out, model, RDFFormat.TURTLE_PRETTY)
        }
        "yay"
      })
    })


    val stopwatch = Stopwatch.createStarted()
    val evalResult = executiveRdd.count
    logger.info("Processed " + evalResult + " items in " + (stopwatch.stop.elapsed(TimeUnit.MILLISECONDS) * 0.001) + " seconds")

      // Set up a dataset processing expression
//      logger.info("Conjure spec is:");
//      RDFDataMgr.write(System.err, effectiveWorkflow.getModel(), RDFFormat.TURTLE_PRETTY);
/*
      try(RdfDataObject data = effectiveWorkflow.accept(executor)) {
        try(RDFConnection conn = data.openConnection()) {
          // Print out the data that is the process result
          Model model = conn.queryConstruct("CONSTRUCT WHERE { ?s ?p ?o }");

          RDFDataMgr.write(System.out, model, RDFFormat.TURTLE_PRETTY);
        }
      } catch(Exception e) {
        logger.warn("Failed to process " + url, e);
      }
    }
*/



//    logger.info(testrdd.count)
    if(false) {

      val it = Seq.range(0, 1000)
        .map(i => s"CONSTRUCT WHERE { ?s$i ?p ?o }")

      val rdd = sparkSession.sparkContext.parallelize(it)



      // What we need:
      // The set of datasets that should be operated upon
      // For each worker, the set of valid local datasets
      // Remote download of hdt files


      val statusReports = rdd
        .mapPartitions(it => mapWithConnection(hostToPortRangesBroadcast)(it)((item, conn) => {
          logger.info(TaskContext.getPartitionId() + " processing " + item)
          val model = conn.queryConstruct(item)
          val baos = new ByteArrayOutputStream
          RDFDataMgr.write(baos, model, RDFFormat.TURTLE_PRETTY)
          val str = baos.toString("UTF-8")
          (item, str)
        }))
        .collect

      logger.info("RESULTS: ----------------------------")
      logger.info("Num results: " + statusReports.length)
      for (item <- statusReports) {
        // logger.info(item)
      }
    }

    sparkSession.stop
    sparkSession.close()


    logger.info("Done")
  }


  //  def wrapperFactory[T, X]


  /**
    * Util function that performs life-cycle management of a SPARQL endpoint for items in a partition
    * @param hostToPortRangesBroadcast Function that yields for a host name the allowed port ranges for spawning SPARQL endpoints
    * @param it Iterator of items in the partition
    * @param fn User defined map function that takes an item of the partition and an RDFConnection as input
    * @tparam T Item type in the partition
    * @tparam X Return type of the user defined map function
    * @return
    */
  def mapWithConnection[T, X](hostToPortRangesBroadcast: Broadcast[String => ImmutableRangeSet[Integer]])
                             (it: Iterator[T])
                             (fn: (T, RDFConnection) => X): Iterator[X] = {
    val hostName = InetAddress.getLocalHost.getHostName

    val hostToPortRanges = hostToPortRangesBroadcast.value
    val portRanges = hostToPortRanges(hostName)
    logger.info("Port ranges: " + portRanges)

    val (server, url) = startSparqlEndpoint(portRanges)

    logger.info(TaskContext.getPartitionId()  + " Got endpoint at " + url)

    val conn = RDFConnectionRemote.create()
      .destination(url.toString)
      .build()

    val onClose = () => {
      logger.info(TaskContext.getPartitionId() + " stopping server")
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
