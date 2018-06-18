package net.sansa_stack.rdf.spark.io.ntriples

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
  * The data source for handling N-Triples, i.e. reading from and writing to disk.
  *
  * @author Lorenz Buehmann
  */
class NTriplesDataSource
  extends DataSourceRegister
    with RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  lazy val conf: Config = ConfigFactory.load("rdf_loader")

  override def shortName(): String = "ntriples"

  // Used for reading from file without a given schema
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation =
    new NTriplesRelation(parameters("path"), null, ParseMode.withName(conf.getString("rdf.ntriples.parser").toUpperCase))(sqlContext)

  // Used for reading from file with a given schema
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation =
    new NTriplesRelation(parameters("path"), schema, ParseMode.withName(conf.getString("rdf.ntriples.parser").toUpperCase))(sqlContext)

  // Used for writing to disk
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val path = checkPath(parameters) // maybe throw an exception/error?
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    val doSave = if (fs.exists(fsPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName} !")
        case SaveMode.Overwrite =>
          fs.delete(fsPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"Given path $path already exists!")
        case SaveMode.Ignore => false
        case _ =>
          throw new IllegalStateException(s"Unsupported save mode ${mode} ")
      }
    } else {
      true
    }

    // save only if there was no failure with the path before
    if(doSave) {
      val ntriplesRDD = data.rdd.map(row => {
        row.toSeq.map(value => value.toString).mkString(" ") + " ."
      })

      ntriplesRDD.saveAsTextFile(path)
    }

    createRelation(sqlContext, parameters, data.schema)
  }

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for N-Triples data."))
  }
}
