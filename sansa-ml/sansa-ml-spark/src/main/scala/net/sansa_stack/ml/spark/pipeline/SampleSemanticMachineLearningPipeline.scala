package net.sansa_stack.ml.spark.utils

import scala.collection.JavaConverters._
import net.sansa_stack.query.spark.ontop.OntopSPARQLEngine
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.query.Query
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.lang.ParserSPARQL11
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{AtomicType, BooleanType, DataTypes, DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer




object SampleSemanticMachineLearningPipeline {
  def main(args: Array[String]): Unit = {

    // setup spark session
    val spark = SparkSession.builder
      .appName(s"rdf2feature")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify",
        "net.sansa_stack.query.spark.ontop.KryoRegistratorOntop"))
      .config("spark.sql.crossJoin.enabled", true)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputFilePath = "/Users/carstendraschner/GitHub/SANSA-Stack/sansa-ml/sansa-ml-spark/src/main/resources/test.ttl"

    val queryString = """
      |SELECT ?seed ?seed__down_age ?seed__down_name
      |
      |WHERE {
      |?seed a <http://dig.isi.edu/Person>
      |OPTIONAL {
      | ?seed <http://dig.isi.edu/age> ?seed__down_age .
      |}
      |OPTIONAL {
      | ?seed <http://dig.isi.edu/name> ?seed__down_name .
      |}
      |}""".stripMargin

    val parser = new ParserSPARQL11()

    val query: Query = parser.parse(new Query(), queryString)

    val projectionVars: Seq[Var] = query.getProjectVars.asScala

    println(projectionVars)

    // load RDF to Dataframe
    val df: DataFrame = spark.read.rdf(Lang.TURTLE)(inputFilePath).cache()
    val dataset = df.toDS()

    implicit val tripleEncoder = Encoders.kryo(classOf[Triple])

    val partitions: Map[RdfPartitionComplex, RDD[Row]] =
      RdfPartitionUtilsSpark.partitionGraph(
        dataset.as[Triple].rdd,
        partitioner = RdfPartitionerComplex(false))

    val sparqlEngine = OntopSPARQLEngine(spark, partitions, Option.empty)
    val bindings: RDD[Binding] = sparqlEngine.execSelect(queryString)

    val features: RDD[Seq[Node]] = bindings.map(binding => {
      projectionVars.map(projectionVar => binding.get(projectionVar))
    })
    features.foreach(println(_))

    val columns: Seq[String] = projectionVars.map(_.toString().replace("?", ""))
    println(columns)

    println("try to identify types")
    val types = features.map(seq => {
      seq.map(node => {
        if (node.isLiteral) {
          // TODO also include possiblity like Datetime and others
          if (node.getLiteralValue.isInstanceOf[Float]) FloatType
          else if (node.getLiteralValue.isInstanceOf[Double]) DoubleType
          else if (node.getLiteralValue.isInstanceOf[Boolean]) BooleanType
          else if (node.getLiteralValue.isInstanceOf[String]) StringType
          else if (node.getLiteralValue.isInstanceOf[Int]) IntegerType
          else StringType
        }
        else StringType
      })
    })
    types.foreach(println(_))
    println()
    println("collaps data types")
    println("first row")
    println(types.take(1).toSeq)
    var columnTypes = ListBuffer.empty[org.apache.spark.sql.types.DataType]
    val firstRow = types.take(1).toSeq
    val numberColumns = firstRow(0).toSeq.size
    println(f"We have $numberColumns columns")
    for (i <- 0 to numberColumns - 1) {
      val allTypesOfColumn = types.map(_(i))
      if (allTypesOfColumn.distinct.collect().toSeq.size == 1) {
        val elements = allTypesOfColumn.take(1).toSeq
        val element = elements(0).asInstanceOf[org.apache.spark.sql.types.DataType] // .toString
        columnTypes.append(element)
      }
      else {
        println("the column type is not clear because different types occured")
        println("the type distribution is:")
        // allTypesOfColumn.foreach(println(_))
        val typeDistribution = allTypesOfColumn.groupBy(identity).mapValues(_.size)
        typeDistribution.foreach(println(_))
        println("this is why we fallback to StringType")
        val element = StringType.asInstanceOf[org.apache.spark.sql.types.DataType]  // .toString
        columnTypes.append(element)
      }
    }
    println("These should be column types")
    println(columnTypes)

    println("Now we create our schema")
    val structTypesList = ListBuffer.empty[StructField]
    for (i <- 0 to numberColumns - 1) {
      structTypesList.append(
        StructField(columns(i), columnTypes(i), nullable = true)
      )
    }
    val schema = StructType(structTypesList.toSeq)
    println(schema)

    println("native scala:")
    val featuresNativeScalaDataTypes = features.map(seq => {
      seq.map(node => {
        node.isLiteral match {
          case true => node.getLiteralValue
          case false => node.toString()
        }
      })
    })
    featuresNativeScalaDataTypes.foreach(println(_))

    // create desired dataframe
    var df1 = spark.createDataFrame(featuresNativeScalaDataTypes.map(Row.fromSeq(_)), schema)
    df1.show(false)
  }
}
