package net.sansa_stack.query.spark.ontop

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.query.spark.api.domain.QueryExecutionFactorySpark
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.geosparql.implementation.datatype.GeometryDatatype
import org.apache.jena.geosparql.implementation.parsers.wkt.WKTReader
import org.apache.jena.geosparql.implementation.vocabulary.Geo
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.query.ResultSetFormatter
import org.apache.jena.rdf.model.ModelFactory
import org.apache.sedona.core.enums.FileDataSplitter
import org.apache.sedona.core.formatMapper.FormatMapper
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.locationtech.jts.geom.Geometry
import org.scalatest.FunSuite

import java.io.ByteArrayInputStream

class GeoSPARQLTests extends FunSuite with DataFrameSuiteBase {

  var qef: QueryExecutionFactorySpark = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    SedonaSQLRegistrator.registerAll(spark)

    GeometryDatatype.registerDatatypes()

    val model = ModelFactory.createDefaultModel()
    model.read(this.getClass.getClassLoader.getResourceAsStream("geospatial/geodata.ttl"), null, "Turtle")
    val triples = spark.rdf(model)

//    val wktTriples = extractWKTLiteralTriples(triples)
//
//    val df = toDf(wktTriples)
//
//    df.show(false)
//
//    df.createOrReplaceTempView("geo")
//
//    spark.sql( """
//                 |SELECT s, ST_Distance(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), o) AS distance
//                 |FROM geo
//                 |ORDER BY distance DESC
//                 |LIMIT 5
//  """.stripMargin).show(false)


    qef = new QueryEngineFactoryOntop(spark).create(triples)

    val t = spark.catalog.listTables().filter(_.name.endsWith("aswkt_geosparqlp23wktliteral")).head()
    spark.table(s"${t.name}").show(false)
  }

  /**
   * Creates a DataFrame for the given WKT geometry triples.
   * @param wktTriples an RDD of triples which have as object a WKT geometry literal
   * @return a DataFrame for subject and object
   */
  def toDf(wktTriples: RDD[Triple], sColName: String = "s", oColName: String = "o"): DataFrame = {
    // map the triples to rows
    val rowRdd = wktTriples.map[Row](t => {
      val geometryLiteral = t.getObject.getLiteralLexicalForm.trim()
      val wktReader = WKTReader.extract(geometryLiteral)
      val geometry = wktReader.getGeometry
      val s = t.getSubject.toString()
      Row.fromSeq(Seq(s, geometry))
    })

    val cols: Seq[StructField] = Seq(StructField(sColName, StringType)) ++  Seq(StructField(oColName, GeometryUDT))
    val schema = StructType(cols)
    spark.createDataFrame(rowRdd, schema)
  }

  def extractWKTLiteralTriples(triples: RDD[Triple]): RDD[Triple] = {
    triples.filter(_.predicateMatches(Geo.AS_WKT_NODE))
  }

  override def conf(): SparkConf = {
    val conf = super.conf
    conf
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator",
        Seq("net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
          classOf[SedonaKryoRegistrator].getName)
          .mkString(","))
    conf
  }

  test(s"Test Ontop with spatial join") {
    val query =
      """
        |PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
        |PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        |PREFIX geo-sf: <http://www.opengis.net/ont/sf#>
        |
        |SELECT ?s1
        |WHERE {
        |	?s1 geo:hasGeometry ?s1Geo .
        |	?s1Geo geo:asWKT ?geo1 .
        |	FILTER(geof:sfIntersects(?geo1, "POLYGON ((0.0 0.0, 90.0 0.0, 90.0 77.94970848221368, 0.0 77.94970848221368, 0.0 0.0))"^^geo:wktLiteral)) .
        |}
        |""".stripMargin
    runQuery(query)
  }

  test(s"Test Ontop with spatial range query as filter") {
    val query =
      """
        |PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
        |PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        |PREFIX geo-sf: <http://www.opengis.net/ont/sf#>
        |
        |SELECT ?s1Geo
        |WHERE {
        |	?s1Geo geo:asWKT ?geo1 .
        |	FILTER(geof:sfIntersects(?geo1, "POLYGON((-87.43839246372724 38.212152318808016,-87.83390027622724 30.873791964989213,-82.16495496372724 30.911502789997847,-80.75870496372724 38.315665464524635,-87.43839246372724 38.212152318808016))"^^geo:wktLiteral)) .
        |}
        |""".stripMargin
    runQuery(query)
  }

  test(s"Test Ontop with spatial range query as predicate") {
    val query =
      """
        |PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
        |PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        |PREFIX geo-sf: <http://www.opengis.net/ont/sf#>
        |
        |SELECT ?s1Geo
        |WHERE {
        |	?s1Geo geo:asWKT ?geo1 .
        | ?geo1 geof:sfIntersects "POLYGON((-87.43839246372724 38.212152318808016,-87.83390027622724 30.873791964989213,-82.16495496372724 30.911502789997847,-80.75870496372724 38.315665464524635,-87.43839246372724 38.212152318808016))"^^geo:wktLiteral
        |}
        |""".stripMargin
    runQuery(query)
  }

  test(s"Test Ontop with spatial range join") {
    val query =
      """
        |PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
        |PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        |PREFIX geo-sf: <http://www.opengis.net/ont/sf#>
        |
        |SELECT ?s1Geo
        |WHERE {
        |	?s1Geo geo:asWKT ?geo1 .
        | ?s2Geo geo:asWKT ?geo2 .
        |	FILTER(geof:sfIntersects(?geo1, ?geo2))
        |}
        |""".stripMargin
    runQuery(query)
  }

  val s =
    """
      |PREFIX my: <http://example.org/ApplicationSchema#>
      |PREFIX geo: <http://www.opengis.net/ont/geosparql#>
      |PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
      |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      |SELECT (xsd:boolean(?sfIntersects) as ?intersects)
      |WHERE {
      |  my:A geo:hasDefaultGeometry ?aGeom .
      |  ?aGeom geo:asGML ?aGML .
      |  my:D geo:hasDefaultGeometry ?dGeom .
      |  ?dGeom geo:asWKT ?dWKT .
      |  BIND (geof:sfIntersects(?aGML, ?dWKT) as ?sfIntersects)
      |}
      |""".stripMargin



  def runQuery(query: String): Unit = {
    val qe = qef.createQueryExecution(query)
    val rs = qe.execSelect()
    ResultSetFormatter.out(rs)
  }


}

object GeoSPARQL {
  val geoAsWKT: Node = NodeFactory.createURI("http://www.opengis.net/ont/geosparql#asWKT")
  val wktGeometry: Node = NodeFactory.createURI("http://www.w3.org/2003/01/geo/wgs84_pos#geometry")

  val fileDataSplitter = FileDataSplitter.WKT
  val formatMapper = new FormatMapper(fileDataSplitter, false)

  def toGeometry(geomString: String): Geometry = {
    val geometry = formatMapper.readGeometry(geomString)
    geometry
  }
}
