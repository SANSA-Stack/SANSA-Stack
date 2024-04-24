package net.sansa_stack.query.spark.rdd.op

import com.typesafe.scalalogging.LazyLogging
import net.sansa_stack.query.spark.RddOpsImplicits
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.rdf.spark.utils.{DataTypeUtils, SparkSessionUtils}
import org.aksw.jena_sparql_api.rdf.collections.{NodeMapper, NodeMapperDelegating, NodeMapperFromRdfDatatype}
import org.aksw.jenax.arq.analytics.BindingAnalytics
import org.aksw.jenax.arq.schema_mapping.{FieldMapping, SchemaMapperImpl, SchemaMapping, TypePromoterImpl}
import org.aksw.r2rml.common.vocab.R2rmlTerms
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.datatypes.xsd.XSDDateTime
import org.apache.jena.graph.NodeFactory
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.vocabulary.XSD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import java.sql.Timestamp
import java.util
import java.util.Calendar

/**
 * Mapper from SPARQL bindings to DataFrames
 *
 * The actual work is carried out by [[SchemaMapperImpl]] which computes
 * a [[SchemaMapping]] from the provided source schema and its type information
 *
 * The schema mapping holds information about how to compute the values for the target
 * schema by means of SPARQL expressions over the source schema. For example,
 * if a target column 'foo' should carry the languages tags of RDF terms held in source column 'bar'
 * then the definition for 'foo' would be:
 * ?foo = lang(?bar)
 *
 * Note, that column names are represented as SPARQL variables in order to facilitate
 * re-use of SPARQL algebra and expressions as a model (=language) for declarative schema mappings.
 *
 * In addition, the field mapping stores type information for the target column. In this example,
 * the type of bar is xsd:string.
 * Jena's [[TypeMapper]] is used to resolve datatypeIris to [[org.apache.jena.datatypes.RDFDatatype]] instances which in
 * turn give access to the Java class which finally is converted to a spark type via [[DataTypeUtils]].
 *
 * FIXME Rename to ResultSetToFrameMapper?
 */
object RddOfBindingsToDataFrameMapper extends LazyLogging {
  import collection.JavaConverters._

  /**
   * Analyze the given [[ResultSetSpark]] (scans the data!) and use
   * the result to configure a new [[SchemaMapperImpl]] instance
   * with reasonable defaults:
   *
   * TypePromotion promotes e.g. short to int
   * Unbound variables default to string columns (with null values only)
   *
   * @param resultSet
   * @return
   */
  def configureSchemaMapper(resultSet: ResultSetSpark): SchemaMapperImpl = {

    // Create a linked hash set from the list of result vars
    // The schema mapper will respect the order
    val javaResultVars = new util.LinkedHashSet(resultSet.getResultVars.asJava)

    // Gather result set statistics using the analytic functions
    val usedDatatypesAndNulls = resultSet.getBindings.javaCollect(
      BindingAnalytics.usedDatatypesAndNullCounts(javaResultVars).asCollector())

    val typeMapper = TypeMapper.getInstance()

    // Supply the statistics to the schema mapper
    val schemaMapper = SchemaMapperImpl.newInstance
      .setSourceVars(javaResultVars)
      .setSourceVarToDatatypes((v: Var) => usedDatatypesAndNulls.get(v).getKey)
      .setSourceVarToNulls((v: Var) => usedDatatypesAndNulls.get(v).getValue)
      .setTypePromotionStrategy(TypePromoterImpl.create)
      .setVarToFallbackDatatypeToString()
      .setTypeRemap(considerDatatypeRemap(_, typeMapper))

    schemaMapper
  }


  /**
   * If a given datatype IRI has no correspondence in Spark then fallback
   * to xsd:string
   *
   * @param tgtType
   * @param typeMapper
   * @return
   */
  def considerDatatypeRemap(tgtType: String, typeMapper: TypeMapper): String = {
    var result = tgtType
    try {
      getNodeToSparkMapper(tgtType, typeMapper)
      // If as spark datatype was obtained then accept the given datatype
    } catch {
      case _: Exception =>
        // Fall back to string
        val fallbackType = XSD.xstring.getURI
        logger.debug("No mapping for %s - falling back to ".format(tgtType, fallbackType))
        result = fallbackType
    }

    result
  }

  /**
   * Attemt to obtain a Spark [[DataType]] for the given datatype IRI w.r.t.
   * to the given [[TypeMapper]]. Raises an exception if no datatype could be
   * obtained
   *
   * @param datatypeIri
   * @param typeMapper
   * @return
   */
  def getNodeToSparkMapper(datatypeIri: String, typeMapper: TypeMapper): NodeToSparkMapper = {
    val effectiveDatatypeIri = getEffectiveDatatype(datatypeIri)

    val registry = TypeMapperRdfSpark.getNodeToSparkMapperRegistry()
    var result = registry.get(effectiveDatatypeIri)

    if (result == null) {
      val rdfDatatype = typeMapper.getSafeTypeByName(effectiveDatatypeIri)
      val javaClass = rdfDatatype.getJavaClass

      if (javaClass == null) {
        throw new IllegalStateException("SchemaMapper: Don't know how to handle: %s. Maybe consider registering a fallback to xsd:string?".format(effectiveDatatypeIri))
      } else {
        val dataType = DataTypeUtils.getSparkType(javaClass)
        val nodeMapper: NodeMapper[Object] = new NodeMapperFromRdfDatatype(rdfDatatype)
        result = new NodeToSparkMapperImpl(dataType, nodeMapper)
      }
    }
    result
  }

  /**
   * Apply a schemaMapping to an RDD[Binding] in order to obtain a
   * corresponding [[DataFrame]]
   *
   * @param bindings
   * @param schemaMapping
   * @return
   */
  def applySchemaMapping(
                          bindings: RDD[Binding],
                          schemaMapping: SchemaMapping): DataFrame = {

    val sparkSession = SparkSessionUtils.getSessionFromRdd(bindings)
    val typeMapper = TypeMapper.getInstance

    val structFields = schemaMapping.getDefinedVars.iterator().asScala.map(v => {
      val fieldMapping: FieldMapping = schemaMapping.getFieldMapping.get(v)

      val datatypeIri = fieldMapping.getDatatypeIri

      val name = v.getVarName
      val nodeToSparkMapper = getNodeToSparkMapper(datatypeIri, typeMapper)
      val dataType = nodeToSparkMapper.getSparkDatatype
      val isNullable = fieldMapping.isNullable

      StructField(name, dataType, isNullable)
    }).toSeq

    val targetSchema = StructType(structFields)

    logger.debug("Created target schema: " + targetSchema)
    // println(targetSchema)

    val rows: RDD[Row] = bindings.mapPartitions(it => {
      // Set up the nodeToSparkMappers; setting those up here avoids
      // serialization issues
      val nodeToSparkMappers = schemaMapping.getDefinedVars.iterator().asScala.map(v => {
        val fieldMapping: FieldMapping = schemaMapping.getFieldMapping.get(v)
        val datatypeIri = fieldMapping.getDatatypeIri
        getNodeToSparkMapper(datatypeIri, TypeMapper.getInstance())
      }).toSeq

      val rowMapper = row => mapToRow(row, schemaMapping, nodeToSparkMappers)
      it.map(rowMapper)
    })
    sparkSession.createDataFrame(rows, targetSchema)
  }

  /**
   * Returns rr:IRI and rr:BlankNode as xsd:string
   *
   * @param datatypeIri
   * @return
   */
  def getEffectiveDatatype(datatypeIri: String): String = {
    datatypeIri match {
      case R2rmlTerms.IRI => XSD.xstring.getURI
      case R2rmlTerms.BlankNode => XSD.xstring.getURI
      case default => default
    }
  }

  def mapToRow(
                binding: Binding,
                schemaMapping: SchemaMapping,
                nodeToSparkMappers: Seq[NodeToSparkMapper]): Row = {
    val seq = schemaMapping.getDefinedVars.iterator().asScala.zipWithIndex.map { case (v, i) =>

      val nodeToSparkMapper = nodeToSparkMappers(i)

      val fieldMapping = schemaMapping.getFieldMapping.get(v)
      val decisionTreeExpr = fieldMapping.getDefinition

      val node = decisionTreeExpr.eval(binding)
      val rawJavaValue: Any = {
        if (node == null) null
        else if (node.isURI) node.getURI
        else if (node.isBlank) node.getBlankNodeLabel
        else nodeToSparkMapper.getNodeMapper().toJava(node)
/*        else NodeMapperFromRdfDatatype.toJavaCore(node, typeMapper.getSafeTypeByName(
          getEffectiveDatatype(fieldMapping.getDatatypeIri))) */
      }

      // Special handling for BigInteger which needs to be converted to BigDecimal
      val effectiveJavaValue = DataTypeUtils.enforceSparkCompatibility(rawJavaValue)

      effectiveJavaValue
    }.toSeq

    Row.fromSeq(seq)
  }
}
