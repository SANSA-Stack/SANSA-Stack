package net.sansa_stack.query

import java.util.stream.Collector

import org.apache.jena.graph.Triple
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import net.sansa_stack.query.spark.api.domain.QueryExecutionFactorySpark
import net.sansa_stack.query.spark.datalake.DataLakeEngine
import net.sansa_stack.query.spark.ontop.QueryEngineFactoryOntop
import net.sansa_stack.query.spark.semantic.QuerySystem
import net.sansa_stack.query.spark.sparqlify.QueryEngineFactorySparqlify
import net.sansa_stack.rdf.spark.mappings.R2rmlMappedSparkSession
import net.sansa_stack.rdf.spark.rdd.op.RddOps

import scala.reflect.ClassTag

/**
 * Wrap up implicit classes/methods to query RDF data from N-Triples files into either [[Sparqlify]], [[Ontop]] or
 * [[Semantic]] partition strategies.
 *
 * @author Gezim Sejdiu
 */

package object spark {

  object SPARQLEngine extends Enumeration {
    type SPARQLEngine = Value
    val Ontop, Sparqlify = Value
  }

  /**
   * Provides a SPARQL executor backed by a SPARQL engine.
   *
   * @param triples the RDD of triples to work on
   */
  implicit class SPARQLEngineImplicit(val triples: RDD[Triple]) extends Serializable {

    val spark = SparkSession.builder().getOrCreate()

    /**
     * Creates a SPARQL engine working on the given RDD of triples.
     * @param queryEngine the SPARQL query engine
     * @return a SPARQL query execution factory
     */
    def sparql(queryEngine: SPARQLEngine.Value = SPARQLEngine.Sparqlify): QueryExecutionFactorySpark = {
      val queryEngineFactory = queryEngine match {
        case SPARQLEngine.Ontop => new QueryEngineFactoryOntop(spark)
        case SPARQLEngine.Sparqlify => new QueryEngineFactorySparqlify(spark)
      }

      queryEngineFactory.create(triples)
    }
  }

  /**
   * Implicit to create a [[QueryExecutionFactorySpark]] with default
   * configuration based on an [[R2rmlMappedSparkSession]]
   *
   * @param mappedSession the session and mapping information for which
   *                      to configure a SPARRQ engine
   */
  implicit class SPARQLEnginePrePartitionedImplicit(val mappedSession: R2rmlMappedSparkSession) extends Serializable {

    // val spark = mappedSession.sparkSession
    // TODO code style: Is there a reason to not get the spark session from the argument?
    val spark = SparkSession.builder().getOrCreate()

    /**
     * Creates a SPARQL engine working on the given RDD of triples.
     * @param queryEngine the SPARQL query engine
     * @return a SPARQL engine
     */
    def sparql(queryEngine: SPARQLEngine.Value = SPARQLEngine.Sparqlify): QueryExecutionFactorySpark = {
      val queryEngineFactory = queryEngine match {
        case SPARQLEngine.Ontop => new QueryEngineFactoryOntop(spark)
        case SPARQLEngine.Sparqlify => new QueryEngineFactorySparqlify(spark)
      }

      queryEngineFactory.create(None, mappedSession.r2rmlModel)
    }

    def sparqlify(): QueryExecutionFactorySpark = {
      sparql(SPARQLEngine.Sparqlify)
    }

    def ontop(): QueryExecutionFactorySpark = {
      sparql(SPARQLEngine.Ontop)
    }
  }

  /**
   *
   *
   */
  implicit class RddOfBindingsImplicits(rddOfTriple: RDD[_ <: Binding]) {

    // def usedPrefixes(targetSize: Int): mutable.MultiMap[Var, RDFDatatype] = RddOfBindingOps.usedIriPrefixes(rddOfTriple)
  }


  implicit class Semantic(partitions: RDD[String]) extends Serializable {

    /**
     * Semantic partition of and RDF graph
     *
     * @param queryInputPath -- a path to the SPARQL queries.
     */
    def sparql(queryInputPath: String): RDD[String] = {
      new QuerySystem(
        partitions,
        queryInputPath).run()
    }

  }

  implicit class HDT(hdt: DataFrame) extends Serializable {

    import net.sansa_stack.query.spark.hdt._

    /**
     * Querying HDT.
     *
     * @param query a SPARQL query.
     * @return a DataFrame of result set.
     */
    def sparqlHDT(query: String): DataFrame =
      hdt.sparkSession.sql(Sparql2SQL.getQuery(query))

  }

  implicit class DataLake(spark: SparkSession) extends Serializable {

    /**
     * Querying a Data Lake.
     */
    def sparqlDL(sparqlQuery: String, mappingsFile: String, configFile: String): DataFrame = {
      DataLakeEngine.run(sparqlQuery, mappingsFile, configFile, spark)
    }
  }


  /**
   * Implicits for operations applicable to any RDD
   * TODO These are utils common to spark (independent of rdf) - new sansa-common-spark layer?
   */
  implicit class RddOpsImplicits[T: ClassTag](rdd: RDD[T]) {

    def javaCollect[A: ClassTag, R: ClassTag](collector: Collector[_ >: T, A, R]): R =
      RddOps.aggregateUsingJavaCollector(rdd, collector)

  }
}
