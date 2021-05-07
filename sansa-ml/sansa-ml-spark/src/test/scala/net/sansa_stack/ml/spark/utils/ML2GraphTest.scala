package net.sansa_stack.ml.spark.utils

import com.holdenkarau.spark.testing.SharedSparkContext
import net.sansa_stack.ml.spark.featureExtraction.{SmartVectorAssembler, SparqlFrame}
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.jena.graph.Triple
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite


class ML2GraphTest extends FunSuite with SharedSparkContext{

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

  test("Test SmartVectorAssembler") {
    val dataset = getData()

    val queryString = """
        |SELECT ?seed ?seed__down_age ?seed__down_name ?seed__down_hasParent__down_name ?seed__down_hasParent__down_age ?seed__down_hasSpouse__down_name ?seed__down_hasSpouse__down_age
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
        |		?seed <http://dig.isi.edu/hasParent> ?seed__down_hasParent .
        |		?seed__down_hasParent <http://dig.isi.edu/name> ?seed__down_hasParent__down_name .
        |	}
        | OPTIONAL {
        |		?seed <http://dig.isi.edu/hasParent> ?seed__down_hasParent .
        |		?seed__down_hasParent <http://dig.isi.edu/age> ?seed__down_hasParent__down_age .
        |	}
        |  OPTIONAL {
        |		?seed <http://dig.isi.edu/hasSpouse> ?seed__down_hasSpouse .
        |		?seed__down_hasSpouse <http://dig.isi.edu/name> ?seed__down_hasSpouse__down_name .
        |	}
        |
        | OPTIONAL {
        |		?seed <http://dig.isi.edu/hasSpouse> ?seed__down_hasSpouse .
        |		?seed__down_hasSpouse <http://dig.isi.edu/age> ?seed__down_hasSpouse__down_age .
        |	}
        |
        |}""".stripMargin
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString)
      .setCollapsByKey(true)
    val collapsedDf = sparqlFrame
      .transform(dataset)
      .cache()

    collapsedDf.show(false)

    val inputDfSize = collapsedDf.count()

    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn("seed")
      .setLabelColumn("seed__down_age(Single_NonCategorical_Decimal)")
      .setNullReplacement("string", "Hallo")
      .setNullReplacement("digit", -1000)
      .setWord2VecSize(3)
      .setWord2VecMinCount(1)



    val mlReadyDf = smartVectorAssembler
      .transform(collapsedDf)
      .withColumn("label", col("label").cast("double"))
      .cache()

    assert(inputDfSize == mlReadyDf.count())

    assert(mlReadyDf.columns.toSet == Set("entityID", "label", "features"))

    mlReadyDf.show(false)
    mlReadyDf.schema.foreach(println(_))

    val ml2Graph = new ML2Graph()
      .setEntityColumn("entityID")
      .setValueColumn("label")

    val metagraph: RDD[Triple] = ml2Graph.transform(mlReadyDf.select("entityID", "label"))
    metagraph.foreach(println(_))
  }
}
