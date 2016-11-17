package net.sansa_stack.rdf.spark

import scala.collection.JavaConversions.asScalaIterator

import org.aksw.sparqlify.algebra.sql.nodes.SqlOpTable
import org.aksw.sparqlify.backend.postgres.DatatypeToStringCast
import org.aksw.sparqlify.config.syntax.Config
import org.aksw.sparqlify.config.v0_2.bridge.ConfiguratorCandidateSelector
import org.aksw.sparqlify.config.v0_2.bridge.SyntaxBridge
import org.aksw.sparqlify.core.algorithms.CandidateViewSelectorSparqlify
import org.aksw.sparqlify.core.algorithms.ViewDefinitionNormalizerImpl
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBase
import org.aksw.sparqlify.util.SparqlifyUtils
import org.aksw.sparqlify.util.SqlBackendConfig
import org.aksw.sparqlify.validation.LoggerCount
import org.apache.commons.io.IOUtils
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.Node
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.riot.Lang
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.spark.sparqlify.BasicTableInfoProviderSpark
import net.sansa_stack.rdf.common.partition.sparqlify.SparqlifyUtils2
import org.aksw.sparqlify.core.sparql.RowMapperSparqlifyBinding
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import com.typesafe.scalalogging.LazyLogging


object MainPartitioner
  extends LazyLogging
{

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
      .getOrCreate()

    val triplesString =
      """<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://xmlns.com/foaf/0.1/givenName>	"Guy De" .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/influenced>	<http://dbpedia.org/resource/Tobias_Wolff> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/influenced>	<http://dbpedia.org/resource/Henry_James> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/deathPlace>	<http://dbpedia.org/resource/Passy> .
        |<http://dbpedia.org/resource/Charles_Dickens>	<http://xmlns.com/foaf/0.1/givenName>	"Charles"@en .
        |<http://dbpedia.org/resource/Charles_Dickens>	<http://dbpedia.org/ontology/deathPlace>	<http://dbpedia.org/resource/Gads_Hill_Place> .""".stripMargin

    val it = RDFDataMgr.createIteratorTriples(IOUtils.toInputStream(triplesString), Lang.NTRIPLES, "http://example.org/").toArray.toSeq
    //it.foreach { x => println("GOT: " + (if(x.getObject.isLiteral) x.getObject.getLiteralLanguage else "-")) }
    val graphRdd = sparkSession.sparkContext.parallelize(it)

    //val map = graphRdd.partitionGraphByPredicates
    val partitions = RdfPartitionUtilsSpark.partitionGraphByPredicates(graphRdd)


    val config = new Config();

    //val logger = LoggerFactory.getLogger(MainPartitioner.getClass);
    val loggerCount = new LoggerCount(logger.underlying)
    val backendConfig = new SqlBackendConfig(new DatatypeToStringCast(), new SqlEscaperBase("", "")) //new SqlEscaperBacktick())
    val sqlEscaper = backendConfig.getSqlEscaper()
    val typeSerializer = backendConfig.getTypeSerializer()

    val schemaProvider = null
    //SchemaProvider schemaProvider = new SchemaProviderImpl(conn, typeSystem, typeAlias, sqlEscaper);
    val syntaxBridge = new SyntaxBridge(schemaProvider)

    val ers = SparqlifyUtils.createDefaultExprRewriteSystem()
    //OpMappingRewriter opMappingRewriter = SparqlifyUtils.createDefaultOpMappingRewriter(typeSystem);
    //MappingOps mappingOps = SparqlifyUtils.createDefaultMappingOps(typeSystem);
    val mappingOps = SparqlifyUtils.createDefaultMappingOps(ers)
    //OpMappingRewriter opMappingRewriter = new OpMappingRewriterImpl(mappingOps);


    val candidateViewSelector = new CandidateViewSelectorSparqlify(mappingOps, new ViewDefinitionNormalizerImpl());


    //RdfViewSystem system = new RdfViewSystem2();
    ConfiguratorCandidateSelector.configure(config, syntaxBridge, candidateViewSelector, loggerCount);

    val views = partitions.map {
      case (p, rdd) =>
//
        println("Processing: " + p)

        val vd = SparqlifyUtils2.createViewDefinition(p);
        val tableName = vd.getRelation match {
          case o: SqlOpTable => o.getTableName
          case _ => throw new RuntimeException("Table name required - instead got: " + vd)
        }

        val layout = RdfPartitionerDefault.determineLayout(p)
        val xxx = ScalaReflection.schemaFor(layout.schema).dataType.asInstanceOf[StructType]
        println("XXX: " + xxx)
        val df = sparkSession.createDataFrame(rdd, xxx)

        df.createOrReplaceTempView(tableName)
        println(vd)
        config.getViewDefinitions.add(vd)

    }

    val basicTableInfoProvider = new BasicTableInfoProviderSpark(sparkSession)

    val rewriter = SparqlifyUtils.createDefaultSparqlSqlStringRewriter(basicTableInfoProvider, null, config, typeSerializer, sqlEscaper)
    val rewrite = rewriter.rewrite(QueryFactory.create("Select * { ?s ?p ?o }"))
    val sqlQueryStr = rewrite.getSqlQueryString
    //RowMapperSparqlifyBinding rewrite.getVarDefinition
    println("SQL QUERY: " + sqlQueryStr)


    val resultDs = sparkSession.sql(sqlQueryStr)
    resultDs.foreach { x => println("RESULT ROW: " + x) }

    //predicateRdds.foreach(x => println(x._1, x._2.count))

    //println(predicates.mkString("\n"))

    sparkSession.stop()
  }
}
