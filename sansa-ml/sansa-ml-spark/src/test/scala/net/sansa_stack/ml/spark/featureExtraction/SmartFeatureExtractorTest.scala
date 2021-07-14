package net.sansa_stack.ml.spark.featureExtraction

import com.holdenkarau.spark.testing.SharedSparkContext
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite


class SmartFeatureExtractorTest extends FunSuite with SharedSparkContext{

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
    val df = dataset.rdd.toDF()

    df.show()

    val sfe = new SmartFeatureExtractor()
      .setEntityColumnName("s")
    // sfe.transform()

    val feDf = sfe
      .transform(df)
    feDf
      .show(false)

    assert(feDf.columns.toSeq.toSet == Set("s", "age", "hasParent", "hasSpouse", "name", "22-rdf-syntax-ns#type"))
  }
}
