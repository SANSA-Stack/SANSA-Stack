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

case class RdfTerm(t: Int, v: String, lang: String, dt: String)

//import net.sansa_stack.rdf.spark.GraphRDDUtils
//import org.dissect.rdf.spark.io.JenaKryoRegistrator

object MainPartitioner {
  def toLexicalForm(o: Any) = "" + o //NodeFmtLib.str(node)

  def termToNode(term: RdfTerm) = {
    val lexicalForm = toLexicalForm(term.v)
    val result = term.t match {
      case 0 => NodeFactory.createBlankNode(lexicalForm)
      case 1 => NodeFactory.createURI(lexicalForm)
      case 2 => {
        val dt = term.dt
        if (dt != null && !dt.isEmpty()) {
          //logger.warn("Language tag should be null or empty, was '" + dt + "'");
        }
        NodeFactory.createLiteral(lexicalForm, term.lang)
      }
      case 3 => // Typed Literal
        val lang = term.lang
        if (lang != null && !lang.isEmpty()) {
          //logger.warn("Language tag should be null or empty, was '" + lang + "'");
        }
        val dt = TypeMapper.getInstance().getSafeTypeByName(term.dt)
        NodeFactory.createLiteral(lexicalForm, dt)
    }
  }

  def nodeToTerm(node: Node) = {
    var t: Int = 0
    var v: Any = ""
    var lang: String = null
    var dt: String = null

    if (node.isBlank()) {
      t = 0;
      v = node.getBlankNodeId().getLabelString();
    } else if (node.isURI()) {
      t = 1;
      v = node.getURI();
    } else if (node.isLiteral()) {

      v = node.getLiteral().getValue();

      //lex = node.getLiteralLexicalForm();

      dt = node.getLiteralDatatypeURI();
      if (dt == null || dt.isEmpty()) {
        //System.err.println("Treating plain literals as typed ones");
        //logger.warn("Treating plain literals as typed ones");
        t = 2;
        lang = node.getLiteralLanguage();
      } else {
        t = 3;
        dt = node.getLiteralDatatypeURI();
      }
    } else {
      throw new RuntimeException("Should not happen");
    }

    var dtStr = if (dt == null) "" else dt;
    var langStr = if (lang == null) "" else lang;

    RdfTerm(t, "" + v, lang, dt)
  }


  def main(args: Array[String]): Unit = {
    //    val sparkContext = {
    //      val conf = new SparkConf().setAppName("BDE-readRDF").setMaster("local[1]")
    //        //.set("spark.kryo.registrationRequired", "true") // use this for debugging and keeping track of which objects are being serialized.
    //        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //        .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    //
    //      new SparkContext(conf)
    //    }
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
      .getOrCreate()

    //    val sqlContext = new SQLContext(sparkContext)

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

    graphRdd.foreach { x => println("RDD: " + x) }

    //val map = graphRdd.partitionGraphByPredicates
    val partitions = GraphRDDUtils.partitionGraphByPredicates(graphRdd)


    val config = new Config();
    val logger = LoggerFactory.getLogger(MainPartitioner.getClass);
    val loggerCount = new LoggerCount(logger)
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

    //QueryExecutionFactoryEx qef = SparqlifyUtils.createDefaultSparqlifyEngine(dataSource, config, typeSerializer, sqlEscaper, mrs, maxQueryExecutionTime);

    // Schema from case class:
    //import org.apache.spark.sql.catalyst.ScalaReflection
    //val schema = ScalaReflection.schemaFor[TestCase].dataType.asInstanceOf[StructType]

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
//        val tag = layout.schema
//        val m = runtimeMirror(getClass.getClassLoader)
//        //val mirror = tag.mirror
//        val clazz = mirror.runtimeClass(tag.tpe.typeSymbol.asClass)
        val xxx = ScalaReflection.schemaFor(layout.schema).dataType.asInstanceOf[StructType]
        println("XXX: " + xxx)
        //sparkSession.createDataFrame(
        val df = sparkSession.createDataFrame(rdd, xxx)
        //sparkSession.create
        //val r = Row()
//        println("Schema for " + tableName + ": " + df.schema + " basic schema: " + layout.schema)
        df.createOrReplaceTempView(tableName)
        println(vd)
        config.getViewDefinitions.add(vd)

//        // TODO Deal with potential name clashes
//        val tableName = p.getURI.substring(p.getURI.lastIndexOf("/") + 1)
//        println("TableName: " + tableName)
//
//        import sparkSession.implicits._
//
//        val layout = RdfPartition.determineLayout(p)
//        val df = sparkSession.createDataFrame(rdd, layout.schema)
//        //val rddx = rdd.map { case (s, o) => (nodeToTerm(s), nodeToTerm(o)) }
////sparkSession.createDataframe
//        //val ds = rddx.toDS()
//        println("FIELDS: " + df.schema)
//
//        df.createOrReplaceTempView(tableName)
//
//        //ds.printSchema()
//
//        val sqlQueryStr = s"""
//           |SELECT _1.v
//           |AS val
//           |FROM `$tableName`
//           |""".stripMargin
//
////        val flatSchema = DatasetUtils.flattenSchema(ds.schema)
////        val columnNames = flatSchema.keySet.toSeq.asJava
////        val typeMap = flatSchema.map({ case (k, v) => (k, TypeToken.alloc(v)) }).asJava
//
//
//        val items = sparkSession.sql(sqlQueryStr)
//        val basicTableInfo = basicTableInfoProvider.getBasicTableInfo(sqlQueryStr)
//        //println("Result schema: " + basicTableInfoProvider.getBasicTableInfo(sqlQueryStr))
//
//        //items.foreach(x => println("Item: " + x))
//
//        //println("Counting the dataset: " + ds.count())
//
//        val quad = new Quad(Quad.defaultGraphIRI, Vars.s, p, Vars.o)
//        val quadPattern = new QuadPattern()
//        quadPattern.add(quad)
//
//        val es = new E_Equals(new ExprVar(Vars.s), E_RdfTerm.createUri(new ExprVar(Var.alloc("_1.v"))))
//        val eo = new E_Equals(new ExprVar(Vars.o), E_RdfTerm.createUri(new ExprVar(Var.alloc("_2.v"))))
//        val el = new ArrayList[Expr] //new ExprList()
//        el.add(es)
//        el.add(eo)
//
//        val typeMap = basicTableInfo.getRawTypeMap.asScala.map({ case (k, v) => (k, TypeToken.alloc(v)) }).asJava
//
//
//        val schema = new SchemaImpl(new ArrayList[String](basicTableInfo.getRawTypeMap.keySet()), typeMap)
//
//        println("Schema: " + schema)
//
//        val sqlOp = new SqlOpTable(null, tableName)
//
//        val vtd = new ViewTemplateDefinition(quadPattern, el)
//
//        val vd = new ViewDefinition(tableName, vtd, sqlOp, Arrays.asList())
//
//
//        println(vd)
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
