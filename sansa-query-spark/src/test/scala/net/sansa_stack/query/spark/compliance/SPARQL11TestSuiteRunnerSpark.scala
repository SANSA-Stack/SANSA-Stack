package net.sansa_stack.query.spark.compliance

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.jena.graph.NodeFactory
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.{Binding, BindingFactory}
import org.apache.jena.sparql.expr.NodeValue
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.Suite

import net.sansa_stack.query.tests.SPARQLQueryEvaluationTestSuiteRunner

/**
 * SPARQL 1.1 test suite runner on Apache Spark.
 *
 *
 * @author Lorenz Buehmann
 */
abstract class SPARQL11TestSuiteRunnerSpark
  extends SPARQLQueryEvaluationTestSuiteRunner
//
    with org.scalatest.BeforeAndAfterAll
    with SharedSparkContext { self: Suite =>

  @transient private var _spark: SparkSession = _

//  def spark: SparkSession = _spark

  lazy val spark = SparkSession.builder.config(conf.set("spark.sql.crossJoin.enabled", "true")).getOrCreate()

  override def beforeAll(): Unit = {
//    super.beforeAll()
    conf.set("spark.sql.crossJoin.enabled", "true")
    _spark = SparkSession.builder.config(conf).getOrCreate()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
    _spark = null
  }




  /**
   * Convert DataFrame to array of bindings.
   */
  protected def toBindings(df: DataFrame, metadata: AnyRef): Array[Binding] = {
    df.rdd.collect().map(row => toBinding(row, metadata))
  }

  val decimalType = DataTypes.createDecimalType()

  /**
   * Convert single row to a binding.
   */
  protected def toBinding(row: Row, metadata: AnyRef): Binding = {
    val binding = BindingFactory.create()

    val fields = row.schema.fields

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
