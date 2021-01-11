package net.sansa_stack.query.spark.compliance

import com.holdenkarau.spark.testing.SharedSparkContext
import net.sansa_stack.query.tests.SPARQLQueryEvaluationTestSuiteRunner
import org.apache.jena.graph.NodeFactory
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.{Binding, BindingFactory}
import org.apache.jena.sparql.expr.NodeValue
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{ConfigMap, Suite}

/**
 * SPARQL 1.1 test suite runner on Apache Spark.
 *
 *
 * @author Lorenz Buehmann
 */
abstract class SPARQL11TestSuiteRunnerSpark
  extends SPARQLQueryEvaluationTestSuiteRunner
//
    with org.scalatest.BeforeAndAfterAllConfigMap
    with SharedSparkContext { self: Suite =>

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")

  @transient private var _spark: SparkSession = _

//  def spark: SparkSession = _spark

  lazy val spark = SparkSession.builder.config(
    conf
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
  ).getOrCreate()

//  override def beforeAll(): Unit = {
////    super.beforeAll()
//    conf.set("spark.sql.crossJoin.enabled", "true")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
//    _spark = SparkSession.builder.config(conf).master("local[1]").getOrCreate()
//  }

  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected: Boolean = false

  override def beforeAll(configMap: ConfigMap): Unit = {
    conf.set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    _spark = SparkSession.builder.config(conf).master("local[1]").getOrCreate()

    val toIgnore = configMap.get("ignore")
    if (toIgnore.nonEmpty) {
      toIgnore.get.asInstanceOf[String].split(",").map(_.trim).foreach(name => IGNORE_NAMES.add(name))
    }
  }

  override def afterAll(configMap: ConfigMap): Unit = {
    super.afterAll()
    spark.stop()
    _spark = null
  }




  /**
   * Convert DataFrame to array of bindings.
   * The metadata object is just a placeholder to an arbitrary helper
   * object that might be needed to generate the bindings out of the DataFrame.
   *
   * @param df the dataframe
   * @param metadata the metadata
   */
  protected def toBindings(df: DataFrame, metadata: AnyRef): Array[Binding] = {
    df.rdd.collect().map(row => toBinding(row, metadata))
  }

  val decimalType = DataTypes.createDecimalType()

  /**
   * Convert single row to a binding.
   * The metadata object is just a placeholder to an arbitrary helper
   * object that might be needed to generate the binding out of the row.
   *
   * @param row the row to convert
   * @param metadata the metadata
   */
  protected def toBinding(row: Row, metadata: AnyRef): Binding = {
    val binding = BindingFactory.create()

    val fields = row.schema.fields

    // this is the generic way, i.e. for each field in the row we use the datatype to generate the corresponding
    // RDF resource or RDF literal
    //TODO there is an obvious limitation as we cannot really distinguish between a string literal and a URI when the
    // datatype is just a string}
    fields.foreach(f => {
      // check for null value first
      if (row.getAs[String](f.name) != null) {
        val v = Var.alloc(f.name)
        val node = if (f.dataType == StringType && row.getAs[String](f.name).startsWith("http://")) {
          NodeFactory.createURI(row.getAs[String](f.name))
        } else {
          val nodeValue = f.dataType match {
            case DoubleType => NodeValue.makeDouble(row.getAs[Double](f.name))
            case FloatType => NodeValue.makeFloat(row.getAs[Float](f.name))
            case StringType => NodeValue.makeString(row.getAs[String](f.name))
            case IntegerType => NodeValue.makeInteger(row.getAs[Int](f.name))
            case LongType => NodeValue.makeInteger(row.getAs[Long](f.name))
            case ShortType => NodeValue.makeInteger(row.getAs[Long](f.name))
            case BooleanType => NodeValue.makeBoolean(row.getAs[Boolean](f.name))
//            case NullType =>
            case x if x.isInstanceOf[DecimalType] => NodeValue.makeDecimal(row.getAs[java.math.BigDecimal](f.name))
            //        case DateType =>
            case _ => throw new RuntimeException("unsupported Spark data type")
          }
          nodeValue.asNode()
        }

        binding.add(v, node)
      }

    })

    binding
  }

}
