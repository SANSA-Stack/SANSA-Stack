package net.sansa_stack.query.spark.sparqlify

import java.io.ByteArrayInputStream

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory
import org.apache.jena.query.Query
import org.apache.spark.sql.SparkSession
import org.scalatest._

import benchmark.generator.Generator
import benchmark.serializer.SerializerModel
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.aksw.jena_sparql_api.stmt.SparqlQueryParserImpl
import org.apache.jena.query.ResultSetFormatter
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang


class TestRdfPartitionSpark extends FlatSpec {

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
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      .appName("Partitioner test")
      .getOrCreate()

    //val rdfStr: String = """<http://ex.org/Nile> <http://ex.org/length> "6800"^^<http://ex.org/km> ."""
    //val triples: List[Triple] = //RDFDataMgr.createIteratorTriples(getClass.getResourceAsStream("/dbpedia-01.nt"), Lang.NTRIPLES, null).asScala.toList

    val rdfStr: String = """<http://example.org/#Spiderman> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://example.org/#Superhero> ."""
    val triples: List[Triple] = RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(rdfStr.getBytes), Lang.NTRIPLES, null).asScala.toList


    //val triples = model.getGraph.find(Node.ANY, Node.ANY, Node.ANY).toList.asScala
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

            var str = """
PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>

SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3
 ?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4
WHERE {
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> rdfs:label ?label .
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> rdfs:comment ?comment .
}
"""

/*
    str = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT * {
          ?s
            rdfs:label ?l ;
            rdfs:comment ?c
        }
    """
*/

    println(ResultSetFormatter.asText(qef.createQueryExecution(str).execSelect()))


//    val testDriver = new TestDriver();
//    testDriver.processProgramParameters(Array[String]("http://example.org/foobar/sparql"))
//    testDriver.setParameterPool(new LocalSPARQLParameterPool(testDriverParams, testDriver.getSeed))
//    testDriver.setServer(new SPARQLConnection2(qef))
//
//    testDriver.init();
//    testDriver.run()

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