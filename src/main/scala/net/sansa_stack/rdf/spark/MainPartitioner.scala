package net.sansa_stack.rdf.spark

import java.util.Arrays
import java.util.HashMap

import scala.collection.JavaConversions._

import org.aksw.jena_sparql_api.utils.Vars
import org.aksw.sparqlify.algebra.sql.nodes.SqlOpTable
import org.aksw.sparqlify.config.syntax.ViewDefinition
import org.aksw.sparqlify.config.syntax.ViewTemplateDefinition
import org.aksw.sparqlify.core.TypeToken
import org.aksw.sparqlify.core.sql.schema.SchemaImpl
import org.apache.commons.io.IOUtils
import org.apache.jena.graph.Node
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.sparql.core.Quad
import org.apache.jena.sparql.core.QuadPattern
import org.apache.jena.sparql.expr.E_Equals
import org.apache.jena.sparql.expr.ExprList
import org.apache.jena.sparql.expr.ExprVar
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import java.util.ArrayList
import org.apache.jena.sparql.expr.Expr

//import net.sansa_stack.rdf.spark.GraphRDDUtils
//import org.dissect.rdf.spark.io.JenaKryoRegistrator

object MainPartitioner {

  def main(args: Array[String]): Unit = {
    val sparkContext = {
      val conf = new SparkConf().setAppName("BDE-readRDF").setMaster("local[1]")
        //.set("spark.kryo.registrationRequired", "true") // use this for debugging and keeping track of which objects are being serialized.
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")

      new SparkContext(conf)
    }

    val sqlContext = new SQLContext(sparkContext)

    val triplesString =
      """<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://xmlns.com/foaf/0.1/givenName>	"Guy De" .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/influenced>	<http://dbpedia.org/resource/Tobias_Wolff> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/influenced>	<http://dbpedia.org/resource/Henry_James> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/deathPlace>	<http://dbpedia.org/resource/Passy> .
        |<http://dbpedia.org/resource/Charles_Dickens>	<http://xmlns.com/foaf/0.1/givenName>	"Charles"@en .
        |<http://dbpedia.org/resource/Charles_Dickens>	<http://dbpedia.org/ontology/deathPlace>	<http://dbpedia.org/resource/Gads_Hill_Place> .""".stripMargin


    val it = RDFDataMgr.createIteratorTriples(IOUtils.toInputStream(triplesString), Lang.NTRIPLES, "http://example.org/").toSeq
    val graphRdd = sparkContext.parallelize(it)


    //val map = graphRdd.partitionGraphByPredicates
    val predicateRdds = GraphRDDUtils.partitionGraphByPredicates(graphRdd)


    val views = predicateRdds.map { pr =>
      val p = pr._1
      println("Processing: " + p)
      val tableName = p.getURI

      val quad = new Quad(Quad.defaultGraphIRI, Vars.s, p, Vars.o)
      val quadPattern = new QuadPattern()
      quadPattern.add(quad)

      val es = new E_Equals(new ExprVar(Vars.s), new ExprVar(Vars.s))
      val eo = new E_Equals(new ExprVar(Vars.o), new ExprVar(Vars.o))
      val el = new ArrayList[Expr]//new ExprList()
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

    sparkContext.stop()
  }
}
