package net.sansa_stack.ml.spark.featureExtraction

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.scalatest.FunSuite

import net.sansa_stack.query.spark.sparqlify.SparqlifyUtils3
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.apache.jena.graph.Triple
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import net.sansa_stack.query.spark.SPARQLEngine


class SparqlFrameTest extends FunSuite with SharedSparkContext{

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")

  lazy val spark = SparkSession.builder()
    .appName(s"SparqlFrame Transformer Unit Test")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // we need Kryo serialization enabled with some custom serializers
    .config("spark.kryo.registrator", String.join(
      ", ",
      "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
      "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
    .config("spark.sql.crossJoin.enabled", true)
    .getOrCreate()

  private val dataPath = this.getClass.getClassLoader.getResource("utils/test.ttl").getPath
  private def getData() = {
    import net.sansa_stack.rdf.spark.io._
    import net.sansa_stack.rdf.spark.model._

    val df: DataFrame = spark.read.rdf(Lang.TURTLE)(dataPath).cache()
    val dataset = df.toDS()
    dataset
  }

  override def beforeAll() {
    super.beforeAll()
    JenaSystem.init()
    spark.sparkContext.setLogLevel("ERROR")
  }
  test("Test SparqlFrame query extracting two features with sparqlify") {
    val dataset = getData()
    val queryString = """
                        |SELECT ?seed ?seed__down_age ?seed__down_name ?seed__down_hasSpouse__down_name
                        |
                        |WHERE {
                        |	?seed a <http://dig.isi.edu/Person> .
                        |
                        |	OPTIONAL {
                        |		?seed <http://dig.isi.edu/age> ?seed__down_age .
                        |	}
                        |	OPTIONAL {
                        |		?seed <http://dig.isi.edu/name> ?seed__down_name .
                        |	}
                        | OPTIONAL {
                        |		?seed <http://dig.isi.edu/hasSpouse> ?seed__down_hasSpouse .
                        |		?seed__down_hasSpouse <http://dig.isi.edu/name> ?seed__down_hasSpouse__down_name .
                        |	}
                        |}""".stripMargin
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString)
      .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)
    val res: DataFrame = sparqlFrame.transform(dataset)

    res.show(false)

    val ages = res.select("seed__down_age").rdd.map(r => r(0)).collect()
    val names = res.select("seed__down_name").rdd.map(r => r(0)).collect()
    val spouseNames = res.select("seed__down_hasSpouse__down_name").rdd.map(r => r(0)).collect()

    val expectedSchema = StructType(Seq(
      StructField("seed", StringType, false),
      StructField("seed__down_age", DecimalType(38, 0), false),
      StructField("seed__down_name", StringType, false),
      StructField("seed__down_hasSpouse__down_name", StringType, true)
    ))
    assert(spouseNames.toSeq.toSet.filter(_ != null) == Set("John", "Mary"))
    assert(ages.toSet.map((setelement: Any) => setelement.asInstanceOf[java.math.BigDecimal].intValue).toSet == Set(2, 25, 28)) // TODO quick dirty fix when compare results of different numeric datatypes
    assert(names.toSet == Set("Mary", "John", "John Jr."))
    assert(res.schema == expectedSchema)
    assert(res.columns.toSeq.toSet == Set("seed", "seed__down_age", "seed__down_name", "seed__down_hasSpouse__down_name"))

  }

  /* test("Test SparqlFrame query extracting two features with sparqlify") {
    val dataset = getData()
    val queryString = """
                        |SELECT ?seed ?seed__down_age ?seed__down_name ?seed__down_hasSpouse__down_name
                        |
                        |WHERE {
                        |	?seed a <http://dig.isi.edu/Person> .
                        |
                        |	OPTIONAL {
                        |		?seed <http://dig.isi.edu/age> ?seed__down_age .
                        |	}
                        |	OPTIONAL {
                        |		?seed <http://dig.isi.edu/name> ?seed__down_name .
                        |	}
                        |}""".stripMargin

    /* println("Usage of Sparqlify for Query Execution")
    val query = QueryFactory.create(queryString)
    implicit val tripleEncoder = Encoders.kryo(classOf[Triple])
    val partitions = RdfPartitionUtilsSpark.partitionGraph(dataset.as[Triple].rdd)
    val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(spark, partitions)
    val qef: QueryExecutionFactorySparqlifySpark = new QueryExecutionFactorySparqlifySpark(spark, rewriter)
    val qe: QueryExecutionSparqlifySpark = qef.createQueryExecution(query)

    val sparkResultSet = qe.execSelectSpark() // SparkResultSet is a pair of result vars + rdd
    val resultVars : java.util.List[Var] = sparkResultSet.getResultVars
    val javaRdd: JavaRDD[Binding] = sparkResultSet.getRdd
    val scalaRdd : RDD[Binding] = javaRdd.rdd

    scalaRdd.foreach(println(_))

     */

    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString)
      .setQueryExcecutionEngine("ontop")
    val res: DataFrame = sparqlFrame.transform(dataset)
    assert(res.columns.toSeq.toSet == Set("seed", "seed__down_age", "seed__down_name", "seed__down_hasSpouse__down_name"))
    val ages = res.select("seed__down_age").rdd.map(r => r(0)).collect()
    val names = res.select("seed__down_name").rdd.map(r => r(0)).collect()
    val spouseNames = res.select("seed__down_hasSpouse__down_name").rdd.map(r => r(0)).collect()
    assert(spouseNames.toSeq.toSet.filter(_ != null) == Set("John", "Mary"))
    assert(ages.toSeq.toSet == Set(2, 25, 28))
    assert(names.toSeq.toSet == Set("Mary", "John", "John Jr."))
    val expectedSchema = StructType(Seq(StructField("seed", StringType, true),
      StructField("seed__down_age", IntegerType, true),
      StructField("seed__down_name", StringType, true),
      StructField("seed__down_hasSpouse__down_name", StringType, true)
    ))
    assert(res.schema == expectedSchema)
  }

   */

}
