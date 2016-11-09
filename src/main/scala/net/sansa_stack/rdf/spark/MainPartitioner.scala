package net.sansa_stack.rdf.spark

import java.util.ArrayList
import java.util.Arrays
import java.util.HashMap

import scala.collection.JavaConversions.asScalaIterator

import org.aksw.jena_sparql_api.utils.Vars
import org.aksw.sparqlify.algebra.sql.nodes.SqlOpTable
import org.aksw.sparqlify.config.syntax.ViewDefinition
import org.aksw.sparqlify.config.syntax.ViewTemplateDefinition
import org.aksw.sparqlify.core.TypeToken
import org.aksw.sparqlify.core.sql.schema.SchemaImpl
import org.apache.commons.io.IOUtils
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.Node
import org.apache.jena.graph.NodeFactory
import org.apache.jena.riot.Lang
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.sparql.core.Quad
import org.apache.jena.sparql.core.QuadPattern
import org.apache.jena.sparql.expr.E_Equals
import org.apache.jena.sparql.expr.Expr
import org.apache.jena.sparql.expr.ExprVar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import scala.collection.mutable.LinkedHashMap

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

  def resolveDts(schema: StructType, qualifiedName: String, fieldName: String, map: LinkedHashMap[String, String]) {
    val field = schema.apply(fieldName)
    val dt = field.dataType
    dt match {
      case st: StructType => resolve(st, qualifiedName, map)
      case _ => map += (qualifiedName -> dt.simpleString)
    }
  }

  def resolve(schema: StructType, prefix: String = "", map: LinkedHashMap[String, String] = LinkedHashMap[String, String]()): LinkedHashMap[String, String] = {
    schema.fields.foreach { sf =>
      val fieldName = sf.name
      val qualifiedName = prefix + (if (prefix.isEmpty()) "" else ".") + fieldName
      resolveDts(schema, qualifiedName, fieldName, map)
    }
    map
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

    val it = RDFDataMgr.createIteratorTriples(IOUtils.toInputStream(triplesString), Lang.NTRIPLES, "http://example.org/").toSeq
    val graphRdd = sparkSession.sparkContext.parallelize(it)

    //val map = graphRdd.partitionGraphByPredicates
    val predicateRdds = GraphRDDUtils.partitionGraphByPredicates(graphRdd)

    val views = predicateRdds.map {
      case (p, rdd) =>

        println("Processing: " + p)
        val tableName = p.getURI.substring(p.getURI.lastIndexOf("/") + 1)
        println("TableName: " + tableName)

        import sparkSession.implicits._

        val rddx = rdd.map { case (s, o) => (nodeToTerm(s), nodeToTerm(o)) }

        val ds = rddx.toDS()
        println("FIELDS: " + ds.schema)

        ds.createOrReplaceTempView(tableName)
        //ds.printSchema()

        val sqlQueryStr = s"""
           |SELECT _1.v
           |AS val
           |FROM `$tableName`
           |""".stripMargin

        println(sqlQueryStr)
        println("Schema: " + ds.schema)
        println("Schema resolved: " + resolve(ds.schema))

        //        val parts = name.split(".")
        //        var datatype = ds.schema;
        //        parts.foreach { part =>
        //
        //        }
        //
        //        ds.schema.apply("_1").dataType

        val x = ds.schema.apply("_1")
        x match {
          case s: StructField => {
            println("structfield: " + s)
            s.dataType match {
              case y: StructType => {
                println("XXX StructType: " + y)
                val z = y.apply("v")
                println("XXXZ" + z.dataType.simpleString)

              }
              case _ => println("XXX bar")
            }
          }
          //case _: StructType => println("structtype: " + _)
          case _ => println("foo")
        }
        println(x)

        //ds.schema.fields(0).
        //val col = ds.col("_1.v")
        //ds.schema.ap
        //println("col: " + col.expr)

        //val x = ds.schema
        //val y = x.apply("_1")
        //y
        //println("got: " + y)

        println("Dtypes: " + ds.dtypes.mkString(", "))

        val items = sparkSession.sql(sqlQueryStr)

        items.foreach(x => println("Item: " + x))

        print("Counting the dataset: " + ds.count())

        val quad = new Quad(Quad.defaultGraphIRI, Vars.s, p, Vars.o)
        val quadPattern = new QuadPattern()
        quadPattern.add(quad)

        val es = new E_Equals(new ExprVar(Vars.s), new ExprVar(Vars.s))
        val eo = new E_Equals(new ExprVar(Vars.o), new ExprVar(Vars.o))
        val el = new ArrayList[Expr] //new ExprList()
        el.add(es)
        el.add(eo)

        val typeMap = new HashMap[String, TypeToken]()
        typeMap.put("s", TypeToken.alloc("Node"));
        typeMap.put("o", TypeToken.alloc("Node"));
        val schema = new SchemaImpl(Arrays.asList("s", "o"), typeMap)
        val sqlOp = new SqlOpTable(schema, tableName)
        //SqlOp

        val vtd = new ViewTemplateDefinition(quadPattern, el)

        val vd = new ViewDefinition(tableName, vtd, sqlOp, Arrays.asList())

        println(vd)
    }

    predicateRdds.foreach(x => println(x._1, x._2.count))

    //println(predicates.mkString("\n"))

    sparkSession.stop()
  }
}
