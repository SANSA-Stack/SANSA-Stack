package net.sansa_stack.query.spark.rdd.op

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import org.aksw.jenax.arq.schema_mapping.TypePromoterImpl
import org.apache.commons.io.IOUtils
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.sparql.core.Var
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

class RddOfBindingsToDataFrameMapperTests
  extends AnyFunSuite with DataFrameSuiteBase {

    override def conf: SparkConf = {
      val conf = super.conf
      conf
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", String.join(", ",
          "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
      conf
    }


    test("schema-mapper should work correctly with timestamp") {
      import net.sansa_stack.query.spark._
      import net.sansa_stack.rdf.spark.partition._

      import scala.collection.JavaConverters._

      val triplesString =
      """<urn:s1> <urn:p> "2021-02-25T16:30:12Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
        |<urn:s2> <urn:p> "2021-02-26"^^<http://www.w3.org/2001/XMLSchema#date> .
        |<urn:s3> <urn:p> "5"^^<http://www.w3.org/2001/XMLSchema#int> .
        |<urn:s4> <urn:p> "6"^^<http://www.w3.org/2001/XMLSchema#long> .
        |      """.stripMargin

      val it = RDFDataMgr.createIteratorTriples(IOUtils.toInputStream(triplesString, "UTF-8"), Lang.NTRIPLES, "http://example.org/").asScala.toSeq
      var graphRdd: RDD[org.apache.jena.graph.Triple] = spark.sparkContext.parallelize(it)


      val qef = graphRdd.verticalPartition(RdfPartitionerDefault).sparqlify()

      val resultSet = qef.createQueryExecution("SELECT ?o { ?s ?p ?o }")
        .execSelectSpark()

      val schemaMapping = RddOfBindingsToDataFrameMapper
        .configureSchemaMapper(resultSet)
        .setTypePromotionStrategy(TypePromoterImpl.create())
        .setVarToFallbackDatatype((v: Var) => null)
        .createSchemaMapping

      println(schemaMapping)
      val df = RddOfBindingsToDataFrameMapper.applySchemaMapping(resultSet.getBindings, schemaMapping)

      df.show(20)

    }

}
