package net.sansa_stack.rdf.spark.io.ntriples

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

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
    val path = parameters.getOrElse("path", "./output/") // can throw an exception/error, it's just for this tutorial
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    mode match {
      case SaveMode.Append => sys.error("Append mode is not supported by " + this.getClass.getCanonicalName); sys.exit(1)
      case SaveMode.Overwrite => fs.delete(fsPath, true)
      case SaveMode.ErrorIfExists => sys.error("Given path: " + path + " already exists!!"); sys.exit(1)
      case SaveMode.Ignore => sys.exit()
    }

    val ntriplesRDD = data.rdd.map(row => {
      row.toSeq.map(value => value.toString).mkString(" ") + " ."
    })

    ntriplesRDD.saveAsTextFile(path)

    createRelation(sqlContext, parameters, data.schema)
  }
}
