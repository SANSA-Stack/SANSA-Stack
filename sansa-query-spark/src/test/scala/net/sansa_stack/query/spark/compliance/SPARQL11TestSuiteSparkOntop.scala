package net.sansa_stack.query.spark.compliance

import java.util.Properties

import scala.collection.JavaConverters._

import com.google.common.collect.ImmutableSet
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.ResultSetStream
import org.apache.jena.sparql.engine.binding.{Binding, BindingFactory}
import org.apache.jena.sparql.expr.NodeValue
import org.apache.jena.sparql.resultset.SPARQLResult
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import net.sansa_stack.query.spark.ontop.Sparql2Sql

/**
 * SPARQL 1.1 test suite for Ontop-based SPARQL-to-SQL implementation.
 *
 *
 * @author Lorenz Buehmann
 */
class SPARQL11TestSuiteSparkOntop
  extends SPARQL11TestSuiteSpark {

  override lazy val IGNORE = ImmutableSet.of(/* AGGREGATES */
    // TODO: support GROUP_CONCAT
    aggregatesManifest + "agg-groupconcat-01", aggregatesManifest + "agg-groupconcat-02", aggregatesManifest + "agg-groupconcat-03", // TODO: support IF
    aggregatesManifest + "agg-err-02", /* BINDINGS
			 */
    // TODO: fix it (UNDEF involves the notion of COMPATIBILITY when joining)
    bindingsManifest + "values8", bindingsManifest + "values5", /* FUNCTIONS */
    // bnode not supported in SPARQL transformation
    functionsManifest + "bnode01", functionsManifest + "bnode02", // the SI does not preserve the original timezone
    functionsManifest + "hours", functionsManifest + "day", // not supported in SPARQL transformation
    functionsManifest + "if01", functionsManifest + "if02", functionsManifest + "in01", functionsManifest + "in02", functionsManifest + "iri01", // not supported in H2 transformation
    functionsManifest + "md5-01", functionsManifest + "md5-02", // The SI does not support IRIs as ORDER BY conditions
    functionsManifest + "plus-1", functionsManifest + "plus-2", // SHA1 is not supported in H2
    functionsManifest + "sha1-01", functionsManifest + "sha1-02", // SHA512 is not supported in H2
    functionsManifest + "sha512-01", functionsManifest + "sha512-02", functionsManifest + "strdt01", functionsManifest + "strdt02", functionsManifest + "strdt03", functionsManifest + "strlang01", functionsManifest + "strlang02", functionsManifest + "strlang03", functionsManifest + "timezone", // TZ is not supported in H2
    functionsManifest + "tz", /* CONSTRUCT not supported yet */
    // Projection cannot be cast to Reduced in rdf4j
    constructManifest + "constructwhere01", constructManifest + "constructwhere02", constructManifest + "constructwhere03", // problem importing dataset
    constructManifest + "constructwhere04", /* CSV */
    // Sorting by IRI is not supported by the SI
    csvTscResManifest + "tsv01", csvTscResManifest + "tsv02", // different format for number and not supported custom datatype
    csvTscResManifest + "tsv03", /* GROUPING */
    // Multi-typed COALESCE as grouping condition TODO: support it
    groupingManifest + "group04", /* NEGATION
			not supported yet */ negationManifest + "subset-by-exclusion-nex-1", negationManifest + "temporal-proximity-by-exclusion-nex-1", negationManifest + "subset-01", negationManifest + "subset-02", negationManifest + "set-equals-1", negationManifest + "subset-03", negationManifest + "exists-01", negationManifest + "exists-02", // DISABLED DUE TO ORDER OVER IRI
    negationManifest + "full-minuend", negationManifest + "partial-minuend", // TODO: enable it
    negationManifest + "full-minuend-modified", negationManifest + "partial-minuend-modified", /* EXISTS
			not supported yet */ existsManifest + "exists01", existsManifest + "exists02", existsManifest + "exists03", existsManifest + "exists04", existsManifest + "exists05", /* PROPERTY PATH */
    // Not supported: ArbitraryLengthPath
    propertyPathManifest + "pp02", // wrong result, unexpected binding
    propertyPathManifest + "pp06", propertyPathManifest + "pp12", propertyPathManifest + "pp14", propertyPathManifest + "pp16", propertyPathManifest + "pp21", propertyPathManifest + "pp23", propertyPathManifest + "pp25", // Not supported: ZeroLengthPath
    propertyPathManifest + "pp28a", propertyPathManifest + "pp34", propertyPathManifest + "pp35", propertyPathManifest + "pp36", propertyPathManifest + "pp37", /* SERVICE
			not supported yet */ serviceManifest + "service1", // no loading of the dataset
    serviceManifest + "service2", serviceManifest + "service3", serviceManifest + "service4a", serviceManifest + "service5", serviceManifest + "service6", serviceManifest + "service7", /* SUBQUERY
			*/
    // Quad translated as a triple. TODO: fix it
    subqueryManifest + "subquery02", subqueryManifest + "subquery04", // EXISTS is not supported yet
    subqueryManifest + "subquery10", // ORDER BY IRI (for supported by the SI)
    subqueryManifest + "subquery11", // unbound variable: Var TODO: fix it
    subqueryManifest + "subquery12", subqueryManifest + "subquery13", // missing results (TODO: fix)
    subqueryManifest + "subquery14")


  var ontopProperties: Properties = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    ontopProperties = new Properties()
    ontopProperties.load(getClass.getClassLoader.getResourceAsStream("ontop-spark.properties"))
  }

  override def runQuery(query: Query, data: Model): SPARQLResult = {
    // distribute on Spark
    val triplesRDD = spark.sparkContext.parallelize(data.getGraph.find().toList.asScala)

    // convert to SQL
    val (sqlQuery, columnMapping) = Sparql2Sql.asSQL(spark, query.toString(), triplesRDD, ontopProperties)

    // we have to replace some parts of the SQL query // TODO not sure how to do it in Ontop properly
    val sql = sqlQuery.replace("\"", "`")
      .replace("`PUBLIC`.", "")
    println(s"SQL query: $sql")

    // run SQL query
    var resultDF = spark.sql(sql)

    // get the correct column names
    resultDF = columnMapping.foldLeft(resultDF) {
      case (df, (sparqlVar, sqlVar)) => df.withColumnRenamed(sqlVar.getName, sparqlVar.getName)
    }
    // and for whatever reason, Ontop generates sometimes more columns as necessary
    if (query.isSelectType) {
      println("result DF before selection")
      resultDF.show(false)
      val vars = query.getProjectVars.asScala.map(_.getVarName).toList
      resultDF = resultDF.select(vars.head, vars.tail: _*)
    }
    println("result DF")
    resultDF.show(false)
    resultDF.printSchema()

    val result = if (query.isSelectType) {
      // convert to bindings
      val model = ModelFactory.createDefaultModel()
      val vars = resultDF.schema.fieldNames.toList.asJava
      val resultsActual = new ResultSetStream(vars, model, toBindings(resultDF).toList.asJava.iterator())
      new SPARQLResult(resultsActual)
    } else if (query.isAskType) {
      // map DF entry to boolean
      val b = resultDF.collect().head.getAs[Int](0) == 1
      new SPARQLResult(b)
    } else {
      fail("unsupported query type")
      null
    }
    result
  }

  override def toBinding(row: Row): Binding = {
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
