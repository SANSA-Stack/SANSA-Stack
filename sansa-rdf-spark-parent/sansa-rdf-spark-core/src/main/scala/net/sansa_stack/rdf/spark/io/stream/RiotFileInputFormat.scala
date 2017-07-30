package net.sansa_stack.rdf.spark.io.stream

import java.io.{Closeable, IOException, InputStream}
import java.util
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.atomic.AtomicLong

import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.hadoop.fs._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodecFactory}
import org.apache.hadoop.mapreduce.lib.input.{CombineFileInputFormat, CombineFileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.system.{ErrorHandlerFactory, StreamRDFBase}
import org.apache.jena.riot.{RDFLanguages, RDFParser, RiotException}

import scala.collection.JavaConverters._

/**
  * @author Lorenz Buehmann
  */
class RiotFileInputFormat extends CombineFileInputFormat[LongWritable, Triple] with Logging{

  override def isSplitable(context: JobContext, file: Path): Boolean = false

  override def listStatus(job: JobContext): util.List[FileStatus] =
    super.listStatus(job).asScala.filter(fs => RDFLanguages.filenameToLang(fs.getPath.getName) != null).asJava


  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Triple] =
    new RiotRecordReader


  class RiotRecordReader extends RecordReader[LongWritable, Triple] {

    private val key = new AtomicLong

    private var pump: ParserPump = null
    private var current: Triple = null
    private var pumpThread: Thread = null

    override def getCurrentKey: LongWritable = if (current == null) null else new LongWritable(key.get)

    override def getProgress: Float = if (pump == null) 0 else pump.getProgress

    override def nextKeyValue(): Boolean = {
      if (pump == null)
        false
      else {
        current = pump.getNext
        key.incrementAndGet
        current != null
      }
    }

    override def getCurrentValue: Triple = current

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      close()
      pump = new ParserPump(split.asInstanceOf[CombineFileSplit], context)
      pumpThread = new Thread(pump)
      pumpThread.setDaemon(true)
      pumpThread.start()
    }

    override def close(): Unit = {
      if (pump != null) {
        pump.close()
        pump = null
      }
      if (pumpThread != null) {
        pumpThread.interrupt()
        pumpThread = null
      }
    }

    private val NOP: Node = NodeFactory.createURI(":")
    private val END_STATEMENT: Triple = Triple.create(NOP, NOP, NOP)

    val SKIP_INVALID_PROPERTY = "sansa.rdf.parser.skipinvalid"

    class ParserPump(split: CombineFileSplit, context: TaskAttemptContext) extends StreamRDFBase with Closeable with Runnable {
      private var paths: Array[Path] = split.getPaths
      private var size: Long = split.getLength
      private val queue: SynchronousQueue[Triple] = new SynchronousQueue[Triple]
      private var skipInvalid: Boolean = context.getConfiguration.getBoolean(SKIP_INVALID_PROPERTY, false)
      private var ex: Exception = null
      private var finishedSize: Long = 0

      private var baseUri: String = ""
      private var seek: Seekable = null
      private var in: InputStream = null

      @throws[IOException]
      @throws[InterruptedException]
      def getNext = {
        val s = queue.take
        if (ex != null) throw new IOException("Exception while parsing: " + baseUri, ex)
        if (s == END_STATEMENT) null
        else s
      }

      @throws[IOException]
      def getProgress = this.synchronized {
        (finishedSize + seek.getPos).toFloat / size.toFloat
      }

      override def run(): Unit = {
        try {
          val conf = context.getConfiguration
          for (file <- paths) {
            println(file)
            try {
              var parser: RDFParser =
                this.synchronized {
                  if (seek != null) finishedSize += seek.getPos
                  close()
                  this.baseUri = file.toString
                  context.setStatus("Parsing " + baseUri)
                  val fs = file.getFileSystem(conf)
                  val fileIn = fs.open(file)
                  this.seek = fileIn
                  val codec = new CompressionCodecFactory(conf).getCodec(file)
                  if (codec != null)
                    in = codec.createInputStream(fileIn, CodecPool.getDecompressor(codec))
                  else
                    in = fileIn

                  RDFParser.create()
                    .source(in)
                    .lang(RDFLanguages.filenameToLang(baseUri))
                    .errorHandler(ErrorHandlerFactory.errorHandlerNoWarnings)
//                    .base("http://example/base")
                    .build()
                }

              parser.parse(this)

            } catch {
              case e: Exception =>
                if (skipInvalid) logWarning("Exception while parsing RDF", e)
                else throw e
            }
          }
        } catch {
          case e: Exception =>
            ex = e
        } finally try
          queue.put(END_STATEMENT)
        catch {
          case ignore: InterruptedException =>

        }
      }

      override def triple(triple: Triple): Unit = {
        try
          queue.put(triple)
        catch {
          case e: InterruptedException =>
            throw new RiotException(e)
        }
      }

      @throws[IOException]
      override def close(): Unit = {
        if (in != null) {
          in.close()
          in = null
        }
      }
    }

  }
}




