package net.sansa_stack.rdf.spark.io.stream

import java.io.{ Closeable, InputStream, IOException }
import java.util
import java.util.concurrent.{ SynchronousQueue, TimeUnit }
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import net.sansa_stack.rdf.common.annotation.Experimental
import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.{ CodecPool, CompressionCodecFactory }
import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, RecordReader, TaskAttemptContext }
import org.apache.hadoop.mapreduce.lib.input.{ CombineFileInputFormat, CombineFileSplit }
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.apache.jena.riot.{ RDFLanguages, RDFParser, RiotException }
import org.apache.jena.riot.system.{ ErrorHandlerFactory, StreamRDFBase }

/**
 * A custom Hadoop input format that uses the Apache Jena RDF I/O technology (RIOT) to parse
 * RDF data files.
 * Read [[https://jena.apache.org/documentation/io/]]
 * for more details.
 *
 * The following RDF formats are supported by Jena:
 *  - N-Triples
 *  - Turtle
 *  - RDF/XML
 *  - N-Quads
 *  - JSON-LD
 *  - RDF/JSON
 *  - TriG
 *  - TriX
 *  - RDF Binary
 *
 * @since 0.3.0
 * @author Lorenz Buehmann
 */
@Experimental
class RiotFileInputFormat extends CombineFileInputFormat[LongWritable, Triple] with Logging {

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
      if (pump == null) {
        false
      } else {
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
    val NUM_THREADS_PROPERTY = "sansa.rdf.parser.numthreads"

    class ParserPump(split: CombineFileSplit, context: TaskAttemptContext) extends StreamRDFBase with Closeable with Runnable {
      private var paths: Array[Path] = split.getPaths
      private var size: Long = split.getLength
      private val queue: SynchronousQueue[Triple] = new SynchronousQueue[Triple]
      private var skipInvalid: Boolean = context.getConfiguration.getBoolean(SKIP_INVALID_PROPERTY, false)
      private var numThreads: Int = context.getConfiguration.getInt(NUM_THREADS_PROPERTY, 1)
      private var ex: Exception = null
      private var finishedSize: Long = 0

      private var baseUri: String = ""
      private var seek: Seekable = null
      private var in: InputStream = null

      //      @throws[IOException]
      //      @throws[InterruptedException]
      //      def getNext = {
      //        val s = queue.take
      //        if (ex != null) throw new IOException("Exception while parsing: " + baseUri, ex)
      //        if (s == END_STATEMENT) null
      //        else s
      //      }

      //      @throws[IOException]
      //      def getProgress = this.synchronized {
      //        (finishedSize + seek.getPos).toFloat / size.toFloat
      //      }
      //
      //      @throws[IOException]
      //      override def close(): Unit = {
      //        if (in != null) {
      //          in.close()
      //          in = null
      //        }
      //      }

      //      override def triple(triple: Triple): Unit = {
      //        try
      //          queue.put(triple)
      //        catch {
      //          case e: InterruptedException =>
      //            throw new RiotException(e)
      //        }
      //      }

      //      override def run(): Unit = {
      //        try {
      //          val conf = context.getConfiguration
      //
      //          for (file <- paths) {
      //            println(file)
      //            try {
      //              val parser: RDFParser =
      //                this.synchronized {
      //                  if (seek != null) finishedSize += seek.getPos
      //                  close()
      //                  this.baseUri = file.toString
      //                  context.setStatus("Parsing " + baseUri)
      //                  val fs = file.getFileSystem(conf)
      //                  val fileIn = fs.open(file)
      //                  this.seek = fileIn
      //                  val codec = new CompressionCodecFactory(conf).getCodec(file)
      //                  if (codec != null)
      //                    in = codec.createInputStream(fileIn, CodecPool.getDecompressor(codec))
      //                  else
      //                    in = fileIn
      //
      //                  RDFParser.create()
      //                    .source(in)
      //                    .lang(RDFLanguages.filenameToLang(baseUri))
      //                    .errorHandler(ErrorHandlerFactory.errorHandlerNoWarnings)
      //                    //                    .base("http://example/base")
      //                    .build()
      //                }
      //
      //              parser.parse(this)
      //
      //            } catch {
      //              case e: Exception =>
      //                if (skipInvalid) logWarning("Exception while parsing RDF", e)
      //                else throw e
      //            }
      //          }
      //
      //        } catch {
      //          case e: Exception =>
      //            ex = e
      //        } finally try
      //          queue.put(END_STATEMENT)
      //
      //        catch {
      //          case ignore: InterruptedException =>
      //
      //        }
      //      }

      private var ins: Seq[InputStream] = Seq()
      private var seeks: Seq[Seekable] = Seq()
      private var queues: Seq[SynchronousQueue[Triple]] = Seq.fill(paths.length)(new SynchronousQueue())

      @throws[IOException]
      @throws[InterruptedException]
      def getNext: Triple = {
        val s = queue.take
        if (ex != null) throw new IOException("Exception while parsing: " + baseUri, ex)

        //        val s = takeAny()

        if (s == END_STATEMENT) null
        else s
      }

      def takeAny(): Triple = {

        var s: Triple = null

        queues.foreach(queue => {
          if (s == null || s == END_STATEMENT) {
            s = queue.take()
          }
        })

        s
      }

      override def run(): Unit = {
        val pool = java.util.concurrent.Executors.newFixedThreadPool(numThreads)

        try {
          val conf = context.getConfiguration

          for (file <- paths) {
            //            val queue = new SynchronousQueue[Triple]()
            //            queues +:= queue
            //            pool.execute(new ParserRunner(file, conf, queue))
            pool.execute(new ParserRunner(file, conf))
          }
          pool.shutdown()
          pool.awaitTermination(1, TimeUnit.HOURS)
        } catch {
          case e: Exception =>
            ex = e
        } finally {
          try
            queue.put(END_STATEMENT)
          catch {
            case ignore: InterruptedException =>
          }
          pool.shutdownNow()
        }
      }

      @throws[IOException]
      override def close(): Unit = {
        ins.foreach(in => {
          if (in != null) {
            in.close()
          }
        })

      }

      @throws[IOException]
      def getProgress: Float = this.synchronized {
        (finishedSize + seeks.map(_.getPos).sum).toFloat / size.toFloat
      }

      class ParserRunner(
        file: Path,
        conf: Configuration)
        //                         queue: SynchronousQueue[Triple])
        extends StreamRDFBase with Runnable {
        //        val queue: SynchronousQueue[Triple] = new SynchronousQueue[Triple]

        override def run(): Unit = {
          println(file)
          try {
            val parser: RDFParser =
              this.synchronized {
                val baseUri = file.toString
                if (seek != null) finishedSize += seek.getPos
                close()
                context.setStatus("Parsing " + baseUri)
                val fs = file.getFileSystem(conf)
                val fileIn = fs.open(file)
                seeks :+= fileIn
                val codec = new CompressionCodecFactory(conf).getCodec(file)
                val in = if (codec != null) {
                  codec.createInputStream(fileIn, CodecPool.getDecompressor(codec))
                } else fileIn

                ins :+= in

                //                queues :+= queue

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

        override def triple(triple: Triple): Unit = {
          try
            queue.put(triple)
          catch {
            case e: InterruptedException =>
              throw new RiotException(e)
          }
        }
      }
    }

  }
}




