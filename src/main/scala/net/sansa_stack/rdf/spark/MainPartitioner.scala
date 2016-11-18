package net.sansa_stack.rdf.spark

import scala.collection.JavaConversions.asScalaIterator

import org.aksw.sparqlify.algebra.sql.nodes.SqlOpTable
import org.aksw.sparqlify.backend.postgres.DatatypeToStringCast
import org.aksw.sparqlify.config.syntax.Config
import org.aksw.sparqlify.core.algorithms.CandidateViewSelectorSparqlify
import org.aksw.sparqlify.core.algorithms.ViewDefinitionNormalizerImpl
import org.aksw.sparqlify.core.sparql.ItemProcessorSparqlify
import org.aksw.sparqlify.core.sparql.RowMapperSparqlifyBinding
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBase
import org.aksw.sparqlify.util.SparqlifyUtils
import org.aksw.sparqlify.util.SqlBackendConfig
import org.aksw.sparqlify.validation.LoggerCount
import org.apache.commons.io.IOUtils
import org.apache.jena.query.QueryFactory
import org.apache.jena.riot.Lang
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.engine.binding.BindingHashMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import com.typesafe.scalalogging.LazyLogging

import net.sansa_stack.rdf.common.partition.sparqlify.SparqlifyUtils2
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import net.sansa_stack.rdf.spark.sparqlify.BasicTableInfoProviderSpark
import net.sansa_stack.rdf.spark.model.RDFDSL


object MainPartitioner
  extends LazyLogging
{

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.kryo.registrationRequired", "true")
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
    val loggerCount = new LoggerCount(logger.underlying)


    val backendConfig = new SqlBackendConfig(new DatatypeToStringCast(), new SqlEscaperBase("", "")) //new SqlEscaperBacktick())
    val sqlEscaper = backendConfig.getSqlEscaper()
    val typeSerializer = backendConfig.getTypeSerializer()


    val ers = SparqlifyUtils.createDefaultExprRewriteSystem()
    val mappingOps = SparqlifyUtils.createDefaultMappingOps(ers)


    val candidateViewSelector = new CandidateViewSelectorSparqlify(mappingOps, new ViewDefinitionNormalizerImpl());

    val views = partitions.map {
      case (p, rdd) =>
//
        println("Processing: " + p)

        val vd = SparqlifyUtils2.createViewDefinition(p);
        val tableName = vd.getRelation match {
          case o: SqlOpTable => o.getTableName
          case _ => throw new RuntimeException("Table name required - instead got: " + vd)
        }

        val scalaSchema = p.layout.schema
        val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]
        val df = sparkSession.createDataFrame(rdd, sparkSchema)

        df.createOrReplaceTempView(tableName)
        config.getViewDefinitions.add(vd)
    }

    val basicTableInfoProvider = new BasicTableInfoProviderSpark(sparkSession)

    val rewriter = SparqlifyUtils.createDefaultSparqlSqlStringRewriter(basicTableInfoProvider, null, config, typeSerializer, sqlEscaper)
    val rewrite = rewriter.rewrite(QueryFactory.create("Select * { <http://dbpedia.org/resource/Guy_de_Maupassant> ?p ?o }"))
    val sqlQueryStr = rewrite.getSqlQueryString
    //RowMapperSparqlifyBinding rewrite.getVarDefinition
    println("SQL QUERY: " + sqlQueryStr)

    val varDef = rewrite.getVarDefinition.getMap
    val fuck = varDef.entries().iterator().next().getKey

    val resultDs = sparkSession.sql(sqlQueryStr)


    val f = { row: Row => val b = rowToBinding(row)
      ItemProcessorSparqlify.process(varDef, b) }
    val g = RDFDSL.kryoWrap(f)
    //val g = genMapper(f)//RDFDSL.kryoWrap(f)

    val finalDs = resultDs.rdd.map(g)

    finalDs.foreach(b => println("RESULT BINDING: " + b))

    //resultDs.foreach { x => println("RESULT ROW: " + ItemProcessorSparqlify.process(varDef, rowToBinding(x))) }
//    val f = { y: Row =>
//      println("RESULT ROW: " + fuck + " - ")
//    }
//
//    val g = genMapper(f)
//    resultDs.foreach { x => f(x) }
    //resultDs.foreach(genMapper({row: Row => println("RESULT ROW: " + fuck) })
    //resultDs.map(genMapper(row: Row => fuck)).foreach { x => println("RESULT ROW: " + x) }

    //predicateRdds.foreach(x => println(x._1, x._2.count))

    //println(predicates.mkString("\n"))

    sparkSession.stop()
  }

//  def genMapperNilesh(kryoWrapper: KryoSerializationWrapper[(Foo => Bar)])
//               (foo: Foo) : Bar = {
//    kryoWrapper.value.apply(foo)
//}
  def genMapper[A, B](f: A => B): A => B = {
    val locker = com.twitter.chill.MeatLocker(f)
    x => locker.get.apply(x)
  }

  def rowToBinding(row: Row): Binding = {
    val result = new BindingHashMap()

    val fieldNames = row.schema.fieldNames
    row.toSeq.zipWithIndex.foreach { case (v, i) =>
      val fieldName = fieldNames(i)
      val j = i + 1
      RowMapperSparqlifyBinding.addAttr(result, j, fieldName, v)
    }

    result
  }

}
