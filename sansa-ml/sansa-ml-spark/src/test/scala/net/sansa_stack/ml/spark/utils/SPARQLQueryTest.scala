package net.sansa_stack.ml.spark.utils

import com.holdenkarau.spark.testing.SharedSparkContext
import net.sansa_stack.ml.spark.common.CommonKryoSetup
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.jena.vocabulary.RDF
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.scalatest.FunSuite

class SPARQLQueryTest extends FunSuite with SharedSparkContext {

  CommonKryoSetup.initKryoViaSystemProperties();

  lazy val spark = CommonKryoSetup.configureKryo(SparkSession.builder())
    .appName(sc.appName)
    .master(sc.master)
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()

  private val dataPath = this.getClass.getClassLoader.getResource("utils/test_data.nt").getPath
  private def getData() = {
    import net.sansa_stack.rdf.spark.io._
    import net.sansa_stack.rdf.spark.model._

    spark.read.rdf(Lang.NTRIPLES)(dataPath).toDS()
  }

  private val ns = "http://sansa-stack.net/test_ont#"

  override def beforeAll() {
    super.beforeAll()
    JenaSystem.init()
  }

  test("Results of a SPARQL query with one projection variable should be correct") {
    implicit val nodeEncoder = Encoders.kryo(classOf[Node])

    val data: Dataset[Triple] = getData()

    // ----------
    var sparqlQueryString =
    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
    "PREFIX owl: <http://www.w3.org/2002/07/owl#> " +
    "SELECT ?s " +
    "WHERE {" +
    "  ?s rdf:type owl:ObjectProperty " +
    "}"
    var sparqlQuery: SPARQLQuery = SPARQLQuery(sparqlQueryString)

    var res: DataFrame = sparqlQuery.transform(data)
    var resultNodes: Array[Node] = res.as[Node].collect()

    val objProp01 = NodeFactory.createURI(ns + "obj_prop_01")
    val objProp02 = NodeFactory.createURI(ns + "obj_prop_02")
    val objProp03 = NodeFactory.createURI(ns + "obj_prop_03")

    assert(resultNodes.contains(objProp01))
    assert(resultNodes.contains(objProp02))
    assert(resultNodes.contains(objProp03))
    // ----------

    sparqlQueryString =
    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
    "PREFIX owl: <http://www.w3.org/2002/07/owl#> " +
    "SELECT ?s " +
    "WHERE {" +
    "  ?s rdf:type owl:DatatypeProperty " +
    "}"

    sparqlQuery = SPARQLQuery(sparqlQueryString)

    res = sparqlQuery.transform(data)
    resultNodes = res.as[Node].collect()

    val dataProp01 = NodeFactory.createURI(ns + "data_prop_01")
    val dataProp02 = NodeFactory.createURI(ns + "data_prop_02")
    val dataProp03 = NodeFactory.createURI(ns + "data_prop_03")

    assert(resultNodes.contains(dataProp01))
    assert(resultNodes.contains(dataProp02))
    assert(resultNodes.contains(dataProp03))
  }

  test("Results of a SPARQL query with two projection variables should be correct") {
    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node)])

    val data: Dataset[Triple] = getData()

    // ----------
    val sparqlQueryString =
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        "PREFIX owl: <http://www.w3.org/2002/07/owl#> " +
        "SELECT ?s ?o " +
        "WHERE {" +
        "  ?s rdf:type ?o " +
        "}"
    val sparqlQuery: SPARQLQuery = SPARQLQuery(sparqlQueryString)

    val res: DataFrame = sparqlQuery.transform(data)
    val resultNodes: Array[(Node, Node)] = res.as[(Node, Node)].collect()

    val objProp01 = NodeFactory.createURI(ns + "obj_prop_01")
    val objProp02 = NodeFactory.createURI(ns + "obj_prop_02")
    val objProp03 = NodeFactory.createURI(ns + "obj_prop_03")

    val objPropertyClass =
      NodeFactory.createURI("http://www.w3.org/2002/07/owl#ObjectProperty")

    val dataProp01 = NodeFactory.createURI(ns + "data_prop_01")
    val dataProp02 = NodeFactory.createURI(ns + "data_prop_02")
    val dataProp03 = NodeFactory.createURI(ns + "data_prop_03")

    val dataPropertyClass =
      NodeFactory.createURI("http://www.w3.org/2002/07/owl#DatatypeProperty")

    assert(resultNodes.contains((objProp01, objPropertyClass)))
    assert(resultNodes.contains((objProp02, objPropertyClass)))
    assert(resultNodes.contains((objProp03, objPropertyClass)))

    assert(resultNodes.contains((dataProp01, dataPropertyClass)))
    assert(resultNodes.contains((dataProp02, dataPropertyClass)))
    assert(resultNodes.contains((dataProp03, dataPropertyClass)))
  }

  test("Results of a SPARQL query with three projection variables should be correct") {
    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node)])

    val data: Dataset[Triple] = getData()

    // ----------
    val sparqlQueryString =
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        "PREFIX owl: <http://www.w3.org/2002/07/owl#> " +
        "SELECT ?s ?p ?o " +
        "WHERE {" +
        "  ?s ?p ?o . " +
        "}"
    val sparqlQuery: SPARQLQuery = SPARQLQuery(sparqlQueryString)

    val res: DataFrame = sparqlQuery.transform(data)
    val resultNodes: Array[(Node, Node, Node)] = res.as[(Node, Node, Node)].collect()

    val objProp01 = NodeFactory.createURI(ns + "obj_prop_01")
    val objProp02 = NodeFactory.createURI(ns + "obj_prop_02")
    val objProp03 = NodeFactory.createURI(ns + "obj_prop_03")

    val objPropertyClass =
      NodeFactory.createURI("http://www.w3.org/2002/07/owl#ObjectProperty")

    val dataProp01 = NodeFactory.createURI(ns + "data_prop_01")
    val dataProp02 = NodeFactory.createURI(ns + "data_prop_02")
    val dataProp03 = NodeFactory.createURI(ns + "data_prop_03")

    val dataPropertyClass =
      NodeFactory.createURI("http://www.w3.org/2002/07/owl#DatatypeProperty")

    assert(resultNodes.contains((objProp01, RDF.`type`.asNode(), objPropertyClass)))
    assert(resultNodes.contains((objProp02, RDF.`type`.asNode(), objPropertyClass)))
    assert(resultNodes.contains((objProp03, RDF.`type`.asNode(), objPropertyClass)))

    assert(resultNodes.contains((dataProp01, RDF.`type`.asNode(), dataPropertyClass)))
    assert(resultNodes.contains((dataProp02, RDF.`type`.asNode(), dataPropertyClass)))
    assert(resultNodes.contains((dataProp03, RDF.`type`.asNode(), dataPropertyClass)))
  }

  test("Results of a SPARQL query with four projection variables should be correct") {
    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node, Node)])

    val data: Dataset[Triple] = getData()

    // ----------
    val sparqlQueryString =
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        "PREFIX owl: <http://www.w3.org/2002/07/owl#> " +
        "SELECT ?s ?p ?p_type ?o " +
        "WHERE {" +
        "  ?s ?p ?o . " +
        "  ?p rdf:type ?p_type " +
        "}"
    val sparqlQuery: SPARQLQuery = SPARQLQuery(sparqlQueryString)

    val res: DataFrame = sparqlQuery.transform(data)
    val resultNodes: Array[(Node, Node, Node, Node)] = res.as[(Node, Node, Node, Node)].collect()

    val objProp01 = NodeFactory.createURI(ns + "obj_prop_01")
    val objProp02 = NodeFactory.createURI(ns + "obj_prop_02")
    val objProp03 = NodeFactory.createURI(ns + "obj_prop_03")

    val objPropertyClass =
      NodeFactory.createURI("http://www.w3.org/2002/07/owl#ObjectProperty")

    val indiv1 = NodeFactory.createURI(ns + "individual_01")
    val indiv2 = NodeFactory.createURI(ns + "individual_02")
    val indiv3 = NodeFactory.createURI(ns + "individual_03")
    val indiv4 = NodeFactory.createURI(ns + "individual_04")
    val indiv5 = NodeFactory.createURI(ns + "individual_05")
    val indiv6 = NodeFactory.createURI(ns + "individual_06")
    val indiv7 = NodeFactory.createURI(ns + "individual_07")
    val indiv8 = NodeFactory.createURI(ns + "individual_08")
    val indiv9 = NodeFactory.createURI(ns + "individual_09")

    assert(resultNodes.contains((indiv1, objProp01, objPropertyClass, indiv7)))
    assert(resultNodes.contains((indiv2, objProp01, objPropertyClass, indiv8)))
    assert(resultNodes.contains((indiv3, objProp01, objPropertyClass, indiv9)))

    assert(resultNodes.contains((indiv4, objProp01, objPropertyClass, indiv7)))
    assert(resultNodes.contains((indiv5, objProp01, objPropertyClass, indiv8)))
    assert(resultNodes.contains((indiv6, objProp01, objPropertyClass, indiv9)))

    assert(resultNodes.contains((indiv4, objProp02, objPropertyClass, indiv1)))
    assert(resultNodes.contains((indiv5, objProp02, objPropertyClass, indiv2)))
    assert(resultNodes.contains((indiv6, objProp02, objPropertyClass, indiv3)))

    assert(resultNodes.contains((indiv7, objProp03, objPropertyClass, indiv3)))
    assert(resultNodes.contains((indiv8, objProp03, objPropertyClass, indiv2)))
    assert(resultNodes.contains((indiv9, objProp03, objPropertyClass, indiv1)))
  }

  test("Results of a SPARQL query with five projection variables should be correct") {
    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node, Node, Node)])

    val data: Dataset[Triple] = getData()

    // ----------
    val sparqlQueryString =
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        "PREFIX owl: <http://www.w3.org/2002/07/owl#> " +
        "SELECT ?s ?p1 ?p_type ?o1 ?o2 " +
        "WHERE {" +
        "  ?s ?p1 ?o1 . " +
        "  ?s ?p2 ?o2 ." +
        "  ?p1 rdf:type ?p_type ." +
        "  FILTER(?p1!=?p2) " +
        "}"
    val sparqlQuery: SPARQLQuery = SPARQLQuery(sparqlQueryString)

    val res: DataFrame = sparqlQuery.transform(data)
    val resultNodes: Array[(Node, Node, Node, Node, Node)] =
      res.as[(Node, Node, Node, Node, Node)].collect()

    val objProp01 = NodeFactory.createURI(ns + "obj_prop_01")
    val objProp02 = NodeFactory.createURI(ns + "obj_prop_02")

    val objPropertyClass =
      NodeFactory.createURI("http://www.w3.org/2002/07/owl#ObjectProperty")

    val indiv1 = NodeFactory.createURI(ns + "individual_01")
    val indiv2 = NodeFactory.createURI(ns + "individual_02")
    val indiv3 = NodeFactory.createURI(ns + "individual_03")
    val indiv4 = NodeFactory.createURI(ns + "individual_04")
    val indiv5 = NodeFactory.createURI(ns + "individual_05")
    val indiv6 = NodeFactory.createURI(ns + "individual_06")
    val indiv7 = NodeFactory.createURI(ns + "individual_07")
    val indiv8 = NodeFactory.createURI(ns + "individual_08")
    val indiv9 = NodeFactory.createURI(ns + "individual_09")

    assert(resultNodes.contains((indiv4, objProp01, objPropertyClass, indiv7, indiv1)))
    assert(resultNodes.contains((indiv5, objProp01, objPropertyClass, indiv8, indiv2)))
    assert(resultNodes.contains((indiv6, objProp01, objPropertyClass, indiv9, indiv3)))

    assert(resultNodes.contains((indiv4, objProp02, objPropertyClass, indiv1, indiv7)))
    assert(resultNodes.contains((indiv5, objProp02, objPropertyClass, indiv2, indiv8)))
    assert(resultNodes.contains((indiv6, objProp02, objPropertyClass, indiv3, indiv9)))
  }
}
