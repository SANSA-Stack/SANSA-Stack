package org.sansa_stack.query.spark.sparqlify

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest._

import benchmark.testdriver.SPARQLConnection2
import benchmark.testdriver.TestDriver
import net.sansa_stack.query.spark.sparqlify.QueryExecutionFactorySparqlifySpark
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import net.sansa_stack.query.spark.sparqlify.SparqlifyUtils3
import org.apache.jena.query.ResultSetFormatter
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.Lang
import benchmark.generator.Generator
import benchmark.serializer.SerializerModel
import org.apache.jena.riot.RDFFormat
import benchmark.testdriver.LocalSPARQLParameterPool
import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory
import org.apache.jena.query.Query
import org.aksw.jena_sparql_api.stmt.SparqlQueryParserImpl
import org.apache.jena.graph.Node

class TestRdfPartition extends FlatSpec {

  "A partitioner" should "support custom datatypes" in {

    val serializer = new SerializerModel()
    Generator.init(Array[String]())
    Generator.setSerializer(serializer)
    Generator.run()
    val testDriverParams = Generator.getTestDriverParams

    val model = serializer.getModel

    //RDFDataMgr.write(System.out, model, RDFFormat.TURTLE)

/*
    val t = typeOf[SchemaStringString]
    val attrNames = t.members.sorted.collect({ case m: MethodSymbol if m.isCaseAccessor => m.name }).toList
    println(attrNames)

    System.exit(0)
*/
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      .appName("Partitioner test")
      .getOrCreate()

    //val rdfStr: String = """<http://ex.org/Nile> <http://ex.org/length> "6800"^^<http://ex.org/km> ."""
    //val triples: List[Triple] = //RDFDataMgr.createIteratorTriples(getClass.getResourceAsStream("/dbpedia-01.nt"), Lang.NTRIPLES, null).asScala.toList
    val triples = model.getGraph.find(Node.ANY, Node.ANY, Node.ANY).toList.asScala
    val graphRdd = sparkSession.sparkContext.parallelize(triples)

    //val graphRdd = NTripleReader.load(sparkSession, "classpath:dbpedia-01.nt")



    val partitions = RdfPartitionUtilsSpark.partitionGraph(graphRdd)
    val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(sparkSession, partitions)
    val qef = FluentQueryExecutionFactory.from(new QueryExecutionFactorySparqlifySpark(sparkSession, rewriter))
      .config()
        .withQueryTransform(new java.util.function.Function[Query, Query] {
            override def apply(qq: Query): Query = { qq.setOffset(Query.NOLIMIT); qq }
        })
        .withParser(SparqlQueryParserImpl.create())
      .end()
      .create()


    val testDriver = new TestDriver();
    testDriver.processProgramParameters(Array[String]("http://example.org/foobar/sparql"))
    testDriver.setParameterPool(new LocalSPARQLParameterPool(testDriverParams, testDriver.getSeed))
    testDriver.setServer(new SPARQLConnection2(qef))

    testDriver.init();
    testDriver.run()

    //println(ResultSetFormatter.asText(qef.createQueryExecution("SELECT * { ?s ?p ?o }").execSelect()))

    sparkSession.stop

    // TODO Validate result - right now its already a success if no exception is thrown

//    val stack = new Stack[Int]
//    stack.push(1)
//    stack.push(2)
//    assert(stack.pop() === 2)
//    assert(stack.pop() === 1)
  }
}