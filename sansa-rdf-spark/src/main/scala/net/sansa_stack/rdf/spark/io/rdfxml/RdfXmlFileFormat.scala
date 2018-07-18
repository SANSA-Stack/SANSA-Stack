package net.sansa_stack.rdf.spark.io.rdfxml

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.jena.rdfxml.xmloutput.impl.Basic
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class RdfXmlFileFormat extends TextBasedFileFormat with DataSourceRegister {
  override val shortName: String = "rdfxml"

  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = {

    val parsedOptions = new RdfXmlOptions(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    val rdfDataSource = RdfXmlDataSource(parsedOptions)

    rdfDataSource.isSplitable && super.isSplitable(sparkSession, options, path)
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] =
    Some(StructType(
      Seq(
        StructField("s", StringType, true),
        StructField("p", StringType, true),
        StructField("o", StringType, true)
      )))


  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val parsedOptions = new RdfXmlOptions(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
//    parsedOptions.compressionCodec.foreach { codec =>
//      CompressionCodecs.setCodecConfiguration(conf, codec)
//    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new RdfXmlOutputWriter(path, parsedOptions, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".rdfxml" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val parsedOptions = new RdfXmlOptions(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    (file: PartitionedFile) => {
      val parser = new JenaParser(parsedOptions)
      RdfXmlDataSource(parsedOptions).readFile(
        broadcastedHadoopConf.value.value,
        file,
        parser)
    }
  }

  override def toString: String = "RDF/XML"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[RdfXmlFileFormat]
}

private[rdfxml] class RdfXmlOutputWriter(
    path: String,
    options: RdfXmlOptions,
    context: TaskAttemptContext)
  extends OutputWriter with Logging {

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path))

  // create the Generator without separator inserted between 2 records
  private[this] val gen = new Basic()

  override def write(row: InternalRow): Unit = {
//    gen.write(row)
//    gen.writeLineEnding()
  }

  override def close(): Unit = {
//    gen.close()
    writer.close()
  }
}
