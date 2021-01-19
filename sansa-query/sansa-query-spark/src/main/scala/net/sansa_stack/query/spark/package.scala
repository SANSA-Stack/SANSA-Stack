package net.sansa_stack.query.spark

import com.google.common.cache.{CacheBuilder, CacheLoader}
import net.sansa_stack.query.spark.api.domain.QueryExecutionFactorySpark
import net.sansa_stack.query.spark.datalake.DataLakeEngine
import net.sansa_stack.query.spark.ontop.OntopSPARQLEngine
import net.sansa_stack.query.spark.ops.rdd.RddOfBindingOps
import net.sansa_stack.query.spark.semantic.QuerySystem
import net.sansa_stack.query.spark.sparqlify.{QueryEngineFactorySparqlify, QueryExecutionSpark, SparkRowMapperSparqlify, SparqlifyUtils3}
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner, RdfPartitionerComplex, RdfPartitionerDefault}
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlModel
import net.sansa_stack.rdf.spark.mappings.R2rmlMappedSparkSession
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import net.sansa_stack.rdf.spark.utils.kryo.io.JavaKryoSerializationWrapper
import org.aksw.sparqlify.core.domain.input.SparqlSqlStringRewrite
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter
import org.apache.jena.datatypes.RDFDatatype
import org.apache.jena.graph.Triple
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.Function
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import org.semanticweb.owlapi.model.OWLOntology

import scala.collection.mutable

/**
 * Wrap up implicit classes/methods to query RDF data from N-Triples files into either [[Sparqlify]] or
 * [[Semantic]] partition strategies.
 *
 * @author Gezim Sejdiu
 */

package object query {

  object SPARQLEngine extends Enumeration {
    type SPARQLEngine = Value
    val Ontop, Sparqlify = Value
  }

  /**
   * A SPARQL executor backed by a SPARQL engine.
   *
   * @param triples the triples to work on
   * @param engine the SPARQL query engine
   */
  class SPARQLExecutor(triples: RDD[Triple], engine: SPARQLEngine.Value = SPARQLEngine.Sparqlify)
    extends QueryExecutor
      with Serializable {

    private val queryExecutor = {
      engine match {
        case SPARQLEngine.Ontop => new OntopSPARQLExecutor(
          new RdfPartitionerComplex(false), triples)
        case SPARQLEngine.Sparqlify => new SparqlifySPARQLExecutor(triples)
      }
    }

    override def sparql(sparqlQuery: String): DataFrame = {
      queryExecutor.sparql(sparqlQuery)
    }

    override def sparqlRDD(sparqlQuery: String): RDD[Binding] = queryExecutor.sparqlRDD(sparqlQuery)
  }

  trait QueryExecutor {
    /**
     * Execute a SPARQL query and return a Spark DataFrame.
     *
     * Default partitioning scheme used is VP.
     *
     * @param sparqlQuery a SPARQL query
     */
    def sparql(sparqlQuery: String): DataFrame


    /**
     * Execute a SPARQL query and return an RDD of bindings.
     *
     * Default partitioning scheme used is VP.
     *
     * @param sparqlQuery a SPARQL query
     */
    def sparqlRDD(sparqlQuery: String): RDD[Binding]
  }

  trait PartitionCache {
    val underlyingGuavaCache = CacheBuilder.newBuilder()
      .maximumSize(10L)
      .build (
        new CacheLoader[RDD[Triple], Map[RdfPartitionStateDefault, RDD[Row]]] {
          def load(triples: RDD[Triple]): Map[RdfPartitionStateDefault, RDD[Row]] =
            RdfPartitionUtilsSpark.partitionGraph(triples, partitioner = RdfPartitionerComplex(false))
        }
      )

    def getOrCreate(triples: RDD[Triple]): Map[RdfPartitionStateDefault, RDD[Row]] = underlyingGuavaCache.get(triples)

  }


  /**
   *
   *
   */
  implicit class RddOfBindingImplicits(rddOfTriple: RDD[_ <: Binding]) {

    // def usedPrefixes(targetSize: Int): mutable.MultiMap[Var, RDFDatatype] = RddOfBindingOps.usedIriPrefixes(rddOfTriple)
  }

  /**
   * An Sparqlify backed SPARQL executor implicitly bound to an RDD[Triple].
   *
   * VP is used as partitioning strategy, i.e. one partition s,o per predicate p in the RDF dataset.
   *
   * @param triples the triples to work on
   */
  implicit class SparqlifySPARQLExecutorAsDefault(triples: RDD[Triple])
    extends QueryExecutor
      with Serializable {

    val queryExecutor = new SPARQLExecutor(triples, engine = SPARQLEngine.Sparqlify)

    override def sparql(sparqlQuery: String): DataFrame = {
      queryExecutor.sparql(sparqlQuery)
    }

    override def sparqlRDD(sparqlQuery: String): RDD[Binding] = queryExecutor.sparqlRDD(sparqlQuery)
  }

  /**
   * An Ontop backed SPARQL executor working on the given RDF partitions.
   *
   * @param partitions the RDF partitions to work on
   * @param ontology an optional ontology containing schema information like classes, class hierarchy, etc. which
   *                 can be used for query optimization as well as OWL QL inference based query rewriting
   */
  class OntopSPARQLExecutor(partitioner: RdfPartitioner[RdfPartitionStateDefault],
                            partitions: Map[RdfPartitionStateDefault, RDD[Row]],
                            ontology: Option[OWLOntology] = None)
    extends QueryExecutor
      with Serializable {

    def this(partitioner: RdfPartitioner[RdfPartitionStateDefault], triples: RDD[Triple]) {
      this(partitioner, RdfPartitionUtilsSpark.partitionGraph(triples, partitioner))
    }

    def this(triples: RDD[Triple]) {
      this(new RdfPartitionerComplex(), triples)
    }

    val spark = SparkSession.builder().getOrCreate()

    val sparqlEngine = OntopSPARQLEngine(spark, partitioner, partitions, ontology = None)

    /**
     * Default partition - using VP.
     *
     * @param sparqlQuery a SPARQL query
     */
    override def sparql(sparqlQuery: String): DataFrame = {
      sparqlEngine.execute(sparqlQuery)
    }

    override def sparqlRDD(sparqlQuery: String): RDD[Binding] = sparqlEngine.execSelect(sparqlQuery)
  }

  implicit class SparqlifySPARQLExecutor2(val mappedSession: R2rmlMappedSparkSession) {
    // extends QueryExecutor
      // with Serializable {

    def sparqlify(): QueryExecutionFactorySpark = {
      val engineFactory = new QueryEngineFactorySparqlify(mappedSession.sparkSession)
      engineFactory.create(null, mappedSession.r2rmlModel)
    }
  }

  /**
   * A Sparqlify backed SPARQL executor working on the given RDF partitions.
   * FIXME Needs porting
   *
   * @param partitions the RDF partitions to work on
   */
  class SparqlifySPARQLExecutor(var partitions: Map[RdfPartitionStateDefault, RDD[Row]])
    extends QueryExecutor
      with Serializable {

    /**
     * A Sparqlify backed SPARQL executor working on the given RDF triples.
     *
     * VP is used as partitioning strategy, i.e. one partition s,o per predicate p in the RDF dataset.
     *
     * @param triples the triples to work on
     */
    def this(triples: RDD[Triple]) {
      this(RdfPartitionUtilsSpark.partitionGraph(triples, RdfPartitionerDefault))
    }

    val spark = SparkSession.builder().getOrCreate()

    val rewriter: SparqlSqlStringRewriter = null // SparqlifyUtils3.createSparqlSqlRewriter(spark, RdfPartitionerDefault, partitions)

    override def sparql(sparqlQuery: String): DataFrame = {
      val query = QueryFactory.create(sparqlQuery)
      val rewrite = rewriter.rewrite(query)
      val df = QueryExecutionSpark.createQueryExecution(spark, rewrite, query)

      df
    }

    override def sparqlRDD(sparqlQuery: String): RDD[Binding] = {
      val query = QueryFactory.create(sparqlQuery)
      val rewrite = rewriter.rewrite(query)
      val df = QueryExecutionSpark.createQueryExecution(spark, rewrite, query)

      val varDef = rewrite.getVarDefinition.getMap

      val rowMapper: Function[Row, Binding] = new SparkRowMapperSparqlify(varDef)
      val z: Function[Row, Binding] = JavaKryoSerializationWrapper.wrap(rowMapper)

      implicit val bindingEncoder: Encoder[Binding] = org.apache.spark.sql.Encoders.kryo[Binding]
      val result: JavaRDD[Binding] = df.javaRDD.map(z)

      result.rdd
    }

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

}
