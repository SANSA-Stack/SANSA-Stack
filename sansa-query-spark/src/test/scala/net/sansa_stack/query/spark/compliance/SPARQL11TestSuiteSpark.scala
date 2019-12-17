package net.sansa_stack.query.spark.compliance

import java.net.{JarURLConnection, URL}
import java.util

import scala.collection.JavaConverters._

import com.google.common.collect.ImmutableSet
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import it.unibz.inf.ontop.test.sparql.ManifestTestUtils
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.resultset.rw.ResultsStAX
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.{Binding, BindingFactory}
import org.apache.jena.sparql.expr.NodeValue
import org.apache.jena.sparql.resultset.{ResultSetCompare, SPARQLResult}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSuite

import net.sansa_stack.rdf.spark.utils.Logging

/**
 * SPARQL 1.1 test suite on Apache Spark.
 *
 *
 * @author Lorenz Buehmann
 */
abstract class SPARQL11TestSuiteSpark
  extends SPARQL11TestSuite
  with DataFrameSuiteBase{


  /**
   * Convert DataFrame to array of bindings.
   */
  protected def toBindings(df: DataFrame): Array[Binding] = {
    df.rdd.collect().map(row => toBinding(row))
  }

  val decimalType = DataTypes.createDecimalType()

  /**
   * Convert single row to a binding.
   */
  protected def toBinding(row: Row): Binding = {
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
