package net.sansa_stack.query.spark.sparqlify.server

import java.io.File

import net.sansa_stack.query.spark.sparqlify.{QueryExecutionFactorySparqlifySpark, SparqlifyUtils3}
import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.aksw.jena_sparql_api.server.utils.FactoryBeanSparqlServer
import org.aksw.sparqlify.core.sparql.RowMapperSparqlifyBinding
import org.apache.commons.io.IOUtils
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.sparql.engine.binding.{Binding, BindingHashMap}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

object MainSansaSparqlServer {

  def main(args: Array[String]): Unit = {

    val tempDirStr = System.getProperty("java.io.tmpdir")
    if (tempDirStr == null) {
      throw new RuntimeException("Could not obtain temporary directory")
    }
    val sparkEventsDir = new File(tempDirStr + "/spark-events")
    if (!sparkEventsDir.exists()) {
      sparkEventsDir.mkdirs()
    }

    // File.createTempFile("spark-events")

    val sparkSession = SparkSession.builder
      .master("local")
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

    val triplesString =
      """<http://dbpedia.org/resource/Guy_de_Maupassant> <http://xmlns.com/foaf/0.1/givenName> "Guy De" .
        |<http://dbpedia.org/resource/Guy_de_Maupassant> <http://example.org/ontology/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant> <http://dbpedia.org/ontology/influenced> <http://dbpedia.org/resource/Tobias_Wolff> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant> <http://dbpedia.org/ontology/influenced> <http://dbpedia.org/resource/Henry_James> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant> <http://dbpedia.org/ontology/deathPlace> <http://dbpedia.org/resource/Passy> .
        |<http://dbpedia.org/resource/Charles_Dickens> <http://xmlns.com/foaf/0.1/givenName> "Charles"@en .
        |<http://dbpedia.org/resource/Charles_Dickens> <http://dbpedia.org/ontology/deathPlace> <http://dbpedia.org/resource/Gads_Hill_Place> .
        |<http://someOnt/1> <http://someOnt/184298> <http://someOnt/272277> .
        |<http://someOnt/184298> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#AnnotationProperty> .
        |<http://snomedct-20170731T150000Z> <http://www.w3.org/2002/07/owl#versionInfo> "20170731T150000Z"@en .
      """.stripMargin

    val it = RDFDataMgr.createIteratorTriples(IOUtils.toInputStream(triplesString, "UTF-8"), Lang.NTRIPLES, "http://example.org/").asScala.toSeq
    // it.foreach { x => println("GOT: " + (if(x.getObject.isLiteral) x.getObject.getLiteralLanguage else "-")) }
    val graphRdd = sparkSession.sparkContext.parallelize(it)

    // val map = graphRdd.partitionGraphByPredicates
    val partitions: Map[RdfPartitionDefault, RDD[Row]] = RdfPartitionUtilsSpark.partitionGraph(graphRdd)

    val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(sparkSession, partitions)

    val qef = new QueryExecutionFactorySparqlifySpark(sparkSession, rewriter)

    val server = FactoryBeanSparqlServer.newInstance.setSparqlServiceFactory(qef).create
    server.join()

    /*
     * val result = graphRdd.partitionGraph().sparql("SELECT * { ?s <http://xmlns.com/foaf/0.1/givenName> ?o ; <http://dbpedia.org/ontology/deathPlace> ?d }")
     */

    //
    //    val q = QueryFactory.create("Select * { ?s <http://xmlns.com/foaf/0.1/givenName> ?o ; <http://dbpedia.org/ontology/deathPlace> ?d }")
    //
    //    val qe = qef.createQueryExecution(q)
    //    println(ResultSetFormatter.asText(qe.execSelect))

    //
    //    val sqlQueryStr = rewrite.getSqlQueryString
    //    //RowMapperSparqlifyBinding rewrite.getVarDefinition
    //    println("SQL QUERY: " + sqlQueryStr)
    //
    //    val varDef = rewrite.getVarDefinition.getMap
    //    val fuck = varDef.entries().iterator().next().getKey
    //
    //    val resultDs = sparkSession.sql(sqlQueryStr)
    //
    //
    //    val f = { row: Row => val b = rowToBinding(row)
    //      ItemProcessorSparqlify.process(varDef, b) }
    //    val g = RDFDSL.kryoWrap(f)
    //    //val g = genMapper(f)//RDFDSL.kryoWrap(f)
    //
    //    val finalDs = resultDs.rdd.map(g)
    //
    //    finalDs.foreach(b => println("RESULT BINDING: " + b))

    // resultDs.foreach { x => println("RESULT ROW: " + ItemProcessorSparqlify.process(varDef, rowToBinding(x))) }
    //    val f = { y: Row =>
    //      println("RESULT ROW: " + fuck + " - ")
    //    }
    //
    //    val g = genMapper(f)
    //    resultDs.foreach { x => f(x) }
    // resultDs.foreach(genMapper({row: Row => println("RESULT ROW: " + fuck) })
    // resultDs.map(genMapper(row: Row => fuck)).foreach { x => println("RESULT ROW: " + x) }

    // predicateRdds.foreach(x => println(x._1, x._2.count))

    // println(predicates.mkString("\n"))

    sparkSession.stop()
  }

  //  def genMapperNilesh(kryoWrapper: KryoSerializationWrapper[(Foo => Bar)])
  //               (foo: Foo) : Bar = {
  //    kryoWrapper.value.apply(foo)
  // }
  def genMapper[A, B](f: A => B): A => B = {
    val locker = com.twitter.chill.MeatLocker(f)
    x => locker.get.apply(x)
  }

  def rowToBinding(row: Row): Binding = {
    val result = new BindingHashMap()

    val fieldNames = row.schema.fieldNames
    row.toSeq.zipWithIndex.foreach {
      case (v, i) =>
        val fieldName = fieldNames(i)
        val j = i + 1
        RowMapperSparqlifyBinding.addAttr(result, j, fieldName, v)
    }

    result
  }

}
