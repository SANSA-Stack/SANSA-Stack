package net.sansa_stack.query.spark.issues

import java.io.StringReader
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperDoubleQuote
import org.apache.jena.graph.Triple
import org.apache.jena.query.{QueryExecutionFactory, ResultSet, ResultSetFactory, ResultSetFormatter, ResultSetRewindable}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.sparql.resultset.ResultSetCompare
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import net.sansa_stack.query.spark._
import net.sansa_stack.query.spark.api.domain.QueryExecutionFactorySpark
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionerComplex, RdfPartitionerDefault}
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition.RDFPartition
import org.aksw.commons.sql.codec.util.SqlCodecUtils

/**
 * https://github.com/SANSA-Stack/SANSA-Stack/issues/102
 *
 * @author Lorenz Buehmann
 */
class Issue101
  extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfterAll {

  override def conf(): SparkConf = {
    val conf = super.conf
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
    conf
  }

  val data =
    """
      |<http://test.com/addressbook#i0432> <http://test.com/addressbook#firstName> "John" .
      |<http://test.com/addressbook#i0432> <http://test.com/addressbook#lastName> "Smith" .
      |<http://test.com/addressbook#i0432> <http://test.com/addressbook#address> _:b1 .
      |
      |_:b1 <http://test.com/addressbook#postalCode> "12345" .
      |_:b1 <http://test.com/addressbook#city> "Springfield" .
      |_:b1 <http://test.com/addressbook#streetAddress> "32 Main st.".
      |_:b1 <http://test.com/addressbook#region> "Alabama" .
      |""".stripMargin

  val query =
    """
      |SELECT ?firstName ?lastName ?postalCode
      |  WHERE {
      |    ?s <http://test.com/addressbook#firstName> ?firstName .
      |    ?s <http://test.com/addressbook#lastName> ?lastName .
      |    ?s <http://test.com/addressbook#address> ?address .
      |
      |    ?address <http://test.com/addressbook#postalCode> ?postalCode .
      |}
      |""".stripMargin

  var triplesRDD: RDD[Triple] = _
  var rsTarget: ResultSetRewindable = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val model = ModelFactory.createDefaultModel()
    model.read(new StringReader(data), null, "N-Triples")
    triplesRDD = spark.rdf(model).cache()

    // local query execution
    val qeTarget = QueryExecutionFactory.create(query, model)
    rsTarget = ResultSetFactory.copyResults(qeTarget.execSelect())
    qeTarget.close()
  }

  test("query on blank nodes - Sparqlify") {
    // Spark query execution
    val qef = triplesRDD.verticalPartition(RdfPartitionerDefault).sparqlify()

    runQuery(qef)
  }

  test("query on blank nodes - Ontop") {
    // Spark query execution
    val qef = triplesRDD.verticalPartition(new RdfPartitionerComplex(),
      explodeLanguageTags = true,
      SqlCodecUtils.createSqlCodecDefault,
      escapeIdentifiers = true).ontop()

    runQuery(qef)
  }

  def runQuery(qef: QueryExecutionFactorySpark): Unit = {
    val qe = qef.createQueryExecution(query)
    val rsActual = qe.execSelect().rewindable()
    qe.close()

    rsTarget.reset()
    assert(ResultSetCompare.equalsByTerm(rsTarget, rsActual), "resultsets do not match")
  }

}
