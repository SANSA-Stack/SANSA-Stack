package net.sansa_stack.query.flink.sparqlify

import com.google.common.collect.HashMultimap
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer
import net.sansa_stack.rdf.flink.partition.core.RdfPartitionUtilsFlink
import net.sansa_stack.rdf.partition.core.RdfPartitionDefault
import net.sansa_stack.rdf.spark.kryo.jena.JenaKryoSerializers._
import net.sansa_stack.rdf.spark.kryo.sparqlify.RestrictedExprSerializer
import org.aksw.jena_sparql_api.views.RestrictedExpr
import org.aksw.sparqlify.util.SparqlifyCoreInit
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.jena.graph.Triple
import org.apache.jena.query.ResultSetFormatter
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.scalatest._

import scala.collection.JavaConverters._
import org.aksw.jena_sparql_api.stmt.SparqlQueryParserImpl
import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory
import benchmark.serializer.SerializerModel
import benchmark.testdriver.TestDriver
import org.apache.jena.query.Query
import benchmark.generator.Generator
import benchmark.testdriver.LocalSPARQLParameterPool
import benchmark.testdriver.SPARQLConnection2
import org.apache.jena.graph.Node
import org.apache.jena.query.Dataset
import net.sansa_stack.rdf.partition.core.RdfPartition
import net.sansa_stack.rdf.partition.schema.SchemaStringString


class TestRdfPartitionFlink extends FlatSpec {

  "A partitioner" should "support custom datatypes" in {
    ExecutionEnvironment.getExecutionEnvironment.getConfig
    SparqlifyCoreInit.initSparqlifyFunctions()
    val env = ExecutionEnvironment.getExecutionEnvironment
    //env.getConfig.addDefaultKryoSerializer(classOf[Binding], classOf[BindingSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[HashMultimap[_,_]], classOf[HashMultimapSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.sparql.core.Var], classOf[VarSerializer])
    //env.getConfig.registerTypeWithKryoSerializer(classOf[org.apache.jena.sparql.core.Var], classOf[VarSerializer])
    env.getConfig.registerKryoType(classOf[net.sansa_stack.rdf.partition.core.RdfPartitionDefault])
    env.getConfig.registerKryoType(classOf[Array[net.sansa_stack.rdf.partition.core.RdfPartitionDefault]])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node], classOf[NodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[Array[org.apache.jena.graph.Node]], classOf[NodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.sparql.core.Var], classOf[VarSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.sparql.expr.Expr], classOf[ExprSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node_Variable], classOf[VariableNodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node_Blank], classOf[NodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node_ANY], classOf[ANYNodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node_URI], classOf[NodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node_Literal], classOf[NodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Triple], classOf[TripleSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[RestrictedExpr], classOf[RestrictedExprSerializer])
    env.getConfig.registerKryoType(classOf[Array[org.apache.jena.graph.Triple]])
    env.getConfig.registerKryoType(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])

    val flinkTable = TableEnvironment.getTableEnvironment(env)


    val serializer = new SerializerModel()
    Generator.init(Array[String]())
    Generator.setSerializer(serializer)
    Generator.run()
    val testDriverParams = Generator.getTestDriverParams
    val model = serializer.getModel
    val triples = model.getGraph.find(Node.ANY, Node.ANY, Node.ANY).toList.asScala




    //val triples = RDFDataMgr.createIteratorTriples(getClass.getResourceAsStream("/dbpedia-01.nt"), Lang.NTRIPLES, null).asScala
      //.map(t => RDFTriple(t.getSubject, t.getPredicate, t.getObject))
    //.toList
    val dsAll: DataSet[Triple] = env.fromCollection(triples)
    val xpartitions: Map[RdfPartitionDefault, DataSet[_ <: Product]] = RdfPartitionUtilsFlink.partitionGraph(dsAll)

    val emptyPartition: RdfPartitionDefault = RdfPartitionDefault(1, "http://ex.org/empty_table", 1, "", false)
    val emptyDataset: DataSet[SchemaStringString] = env.fromCollection(Set[SchemaStringString]())
    val partitions: Map[RdfPartitionDefault, DataSet[_ <: Product]] = xpartitions + (emptyPartition -> emptyDataset)

    val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(env, flinkTable, partitions)




    val qef = FluentQueryExecutionFactory.from(new QueryExecutionFactorySparqlifyFlink(env, flinkTable, rewriter))
      .config()
        .withQueryTransform(new java.util.function.Function[Query, Query] {
            override def apply(qq: Query): Query = { qq.setOffset(Query.NOLIMIT); qq }
        })
        .withParser(SparqlQueryParserImpl.create())
      .end()
      .create()

      val str = """
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
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:producer ?p .
    ?p rdfs:label ?producer .
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> dc:publisher ?p .
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productFeature ?f .
    ?f rdfs:label ?productFeature .
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyTextual1 ?propertyTextual1 .
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyTextual2 ?propertyTextual2 .
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyTextual3 ?propertyTextual3 .
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyNumeric1 ?propertyNumeric1 .
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyNumeric2 ?propertyNumeric2 .
*/

      println(ResultSetFormatter.asText(qef.createQueryExecution(str).execSelect()))

//    val testDriver = new TestDriver();
//    testDriver.processProgramParameters(Array[String]("http://example.org/foobar/sparql"))
//    testDriver.setParameterPool(new LocalSPARQLParameterPool(testDriverParams, testDriver.getSeed))
//    testDriver.setServer(new SPARQLConnection2(qef))

//    testDriver.init();
//    testDriver.run()


//    flinkTable.scan("deathPlace").printSchema();
//    val res = flinkTable.sql(
//      "SELECT CAST(s as VarChar)  FROM deathPlace"
//    )
//    res.toDataSet[Row].print()
    //println(flinkTable.explain(res))
    //ds.print()
    //env.execute()
  }
}