package net.sansa_stack.inference.spark.data.loader.sql

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
  * @author Lorenz Buehmann
  */
class NTriplesDataSource
  extends DataSourceRegister
    with RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  lazy val conf: Config = ConfigFactory.load("rdf_loader")

  override def shortName(): String = "ntriples"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation =
    new NTriplesRelation(parameters("path"), null, conf.getString("rdf.ntriples.parser"))(sqlContext)

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation =
    new NTriplesRelation(parameters("path"), schema, conf.getString("rdf.ntriples.parser"))(sqlContext)

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
