package net.sansa_stack.rdf.spark.io

import org.apache.jena.riot.RDFLanguages
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * A custom DataSource for RDF data.
 *
 * `path` and `lang` params have to be provided via the `parameters` map.
 *
 * @author Lorenz Buehmann
 */
class RDFDataSource extends DataSourceRegister
  with RelationProvider
  with SchemaRelationProvider {

  override def shortName(): String = "rdf"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = createRelation(sqlContext, parameters, null)

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    val langParameter = parameters.get("lang")
    val lang = langParameter match {
      case Some(langStr) => RDFLanguages.shortnameToLang(langStr)
      case None => throw new IllegalArgumentException("The lang parameter cannot be empty!")
    }

    val pathParameter = parameters.get("path")
    pathParameter match {
      case Some(path) => new RDFRelation(path, lang, null)(sqlContext)
      case None => throw new IllegalArgumentException("The path parameter cannot be empty!")
    }
  }
}
