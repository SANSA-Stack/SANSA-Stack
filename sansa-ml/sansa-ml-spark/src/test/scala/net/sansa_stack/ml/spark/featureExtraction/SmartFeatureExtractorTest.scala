package net.sansa_stack.ml.spark.featureExtraction

import com.holdenkarau.spark.testing.SharedSparkContext
import net.sansa_stack.ml.spark.common.CommonKryoSetup
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite


class SmartFeatureExtractorTest extends AnyFunSuite with SharedSparkContext{

  CommonKryoSetup.initKryoViaSystemProperties();

  lazy val spark = CommonKryoSetup.configureKryo(SparkSession.builder())
    .appName(s"SparqlFrame Transformer Unit Test")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // we need Kryo serialization enabled with some custom serializers
    .config("spark.kryo.registrator", String.join(
      ", ",
      "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
      "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
    .config("spark.sql.crossJoin.enabled", true)
    .getOrCreate()

  private val dataPath = this.getClass.getClassLoader.getResource("similarity/sampleMovieDB.nt").getPath
  private def getData() = {
    import net.sansa_stack.rdf.spark.io._
    import net.sansa_stack.rdf.spark.model._

    val df: DataFrame = spark.read.rdf(Lang.TURTLE)(dataPath).cache()
    val dataset = df.toDS()
    dataset
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    JenaSystem.init()
    spark.sparkContext.setLogLevel("ERROR")
  }

  test("Test SmartFeatureExtractor") {
    val dataset: Dataset[graph.Triple] = getData()

    /** Smart Feature Extractor */
    val sfeNoFilter = new SmartFeatureExtractor()
      // .setEntityColumnName("s")
      // .setObjectFilter("http://data.linkedmdb.org/movie/film")
      // .setSparqlFilter("SELECT ?s WHERE { ?s ?p <http://data.linkedmdb.org/movie/film> }")

    /** Feature Extracted DataFrame */
    val feDf = sfeNoFilter
      .transform(dataset)
    feDf
      .show(false)

    val sfeObjectFilter = new SmartFeatureExtractor()
      // .setEntityColumnName("s")
      .setObjectFilter("http://data.linkedmdb.org/movie/film")

    val feDf1 = sfeObjectFilter
      .transform(dataset)
    feDf1
      .show(false)


    val sfeSparqlFilter = new SmartFeatureExtractor()
      // .setEntityColumnName("s")
      .setSparqlFilter("SELECT ?s WHERE { ?s ?p <http://data.linkedmdb.org/movie/film> }")

    val feDf2 = sfeSparqlFilter
      .transform(dataset)
    feDf2
      .show()

    // assert(feDf.columns.toSeq.toSet == Set("s", "age", "hasParent", "hasSpouse", "name", "22-rdf-syntax-ns#type"))
  }
}
