package net.sansa_stack.rdf.common.io.hadoop

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, IOException, InputStream, SequenceInputStream, StringReader}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.Charset
import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.{Matcher, Pattern}

import io.reactivex.Flowable
import io.reactivex.functions.Predicate
import org.aksw.jena_sparql_api.common.DefaultPrefixes
import org.aksw.jena_sparql_api.io.binseach._
import org.aksw.jena_sparql_api.rx.RDFDataMgrRx
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.{CompressedSplitLineReader, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.jena.ext.com.google.common.primitives.Ints
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodec, CompressionCodecFactory, Decompressor, SplitCompressionInputStream, SplittableCompressionCodec}


/**
 * A record reader for Trig RDF files.
 *
 * @author Lorenz Buehmann
 * @author Claus Stadler
 */
object TrigRecordReader {
  val MAX_RECORD_LENGTH = "mapreduce.input.trigrecordreader.record.maxlength"
  val MIN_RECORD_LENGTH = "mapreduce.input.trigrecordreader.record.minlength"
  val PROBE_RECORD_COUNT = "mapreduce.input.trigrecordreader.probe.count"
}
class TrigRecordReader
  extends RecordReader[LongWritable, Dataset] {

  var maxRecordLength: Int = _
  var minRecordLength: Int = _
  var probeRecordCount: Int = _

  private val trigFwdPattern: Pattern = Pattern.compile("@?base|@?prefix|(graph\\s*)?(<[^>]*>|_?:[^-\\s]+)\\s*\\{", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE)

  // This pattern is no longer needed and its not up to date
  private val trigBwdPattern: Pattern = Pattern.compile("esab@?|xiferp@?|\\{\\s*(>[^<]*<|[^-\\s]+:_)\\s*(hparg)?", Pattern.CASE_INSENSITIVE)

  // private var start, end, position = 0L

  private val EMPTY_DATASET: Dataset = DatasetFactory.create

  private val currentKey = new AtomicLong
  private var currentValue: Dataset = DatasetFactory.create()

  private var datasetFlow: util.Iterator[Dataset] = _


  /**
    * Uses the matcher to find candidate probing positions, and returns the first positoin
    * where probing succeeds.
    * Matching ranges are part of the matcher configuration
    *
    * @param rawSeekable
    * @param m
    * @param isFwd
    * @param prober
    * @return
    */
  def findPosition(rawSeekable: Seekable, m: Matcher, isFwd: Boolean, prober: Seekable => Boolean): Long = {

    val seekable = rawSeekable.cloneObject
    val absMatcherStartPos = seekable.getPos

    while (m.find) {
      val start = m.start
      val end = m.end
      // The matcher yields absolute byte positions from the beginning of the byte sequence
      val matchPos = if (isFwd) {
        start
      } else {
        -end + 1
      }
      val absPos = (absMatcherStartPos + matchPos).asInstanceOf[Int]
      // Artificially create errors
      // absPos += 5;
      seekable.setPos(absPos)
      val probeSeek = seekable.cloneObject

      val probeResult = prober.apply(probeSeek)
      // System.err.println(s"Probe result for matching at pos $absPos with fwd=$isFwd: $probeResult")

      if(probeResult) {
        return absPos
      }
    }

    -1L
  }

  private var isCompressedInput = false
  private var decompressor: Decompressor = null


  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    val job = context.getConfiguration

    maxRecordLength = job.getInt(TrigRecordReader.MAX_RECORD_LENGTH, 10 * 1024)
    minRecordLength = job.getInt(TrigRecordReader.MIN_RECORD_LENGTH, 12)
    probeRecordCount = job.getInt(TrigRecordReader.PROBE_RECORD_COUNT, 10)

    val split = inputSplit.asInstanceOf[FileSplit]
    val rawStream = split.getPath.getFileSystem(context.getConfiguration).open(split.getPath)

    var stream: InputStream = rawStream

    var splitStart = split.getStart
    var splitEnd = splitStart + split.getLength

    val rawDesiredBufferLength = split.getLength + Math.min(2 * maxRecordLength + probeRecordCount * maxRecordLength, split.getLength - 1)

    val file = split.getPath

    var fsSeek: fs.Seekable = rawStream



    val codec = new CompressionCodecFactory(job).getCodec(file)
    if (null != codec) {
      isCompressedInput = true
      decompressor = CodecPool.getDecompressor(codec)
      if (codec.isInstanceOf[SplittableCompressionCodec]) {
        val cIn = codec.asInstanceOf[SplittableCompressionCodec].createInputStream(
            stream, decompressor, splitStart, splitEnd,
            SplittableCompressionCodec.READ_MODE.BYBLOCK)

        splitStart = cIn.getAdjustedStart
        splitEnd = cIn.getAdjustedEnd

        fsSeek = cIn
        stream = cIn
      } else {
//        fsSeek.seek(splitStart)
      }
    } else {
      fsSeek.seek(splitStart)
    }






    val str = context.getConfiguration.get("prefixes")
    val model = ModelFactory.createDefaultModel()
    if (str != null) RDFDataMgr.read(model, new StringReader(str), null, Lang.TURTLE)
    val tmp = createDatasetFlowApproachEasyPeasy(splitStart, splitEnd, stream, context, model)

    datasetFlow = tmp.blockingIterable().iterator()
  }

  def skipOverNextRecord(nav: PageNavigator, splitStart: Long, absProbeRegionStart: Long, maxRecordLength: Long, absDataRegionEnd: Long, prober: Seekable => Boolean): Long = {
    var result = -1L

    val availableDataRegion = absDataRegionEnd - absProbeRegionStart
    var nextProbePos = absProbeRegionStart
    var i = 0
    while (i < 2) {
      val candidatePos = findNextRecord(nav, splitStart, nextProbePos, maxRecordLength, absDataRegionEnd, prober)
      if (candidatePos < 0) {
        if (availableDataRegion >= maxRecordLength) {
          throw new RuntimeException(s"Found no record start in a record search region of $maxRecordLength bytes, although $availableDataRegion bytes were available")
        }

        // Retain best found candidate position
        // effectiveRecordRangeEnd = dataRegionEnd
        i = 666 // break
      } else {
        result = candidatePos
        if (i == 0) {
          nextProbePos = candidatePos + minRecordLength
        }
        i += 1
      }
    }

    result
  }

  def findNextRecord(nav: PageNavigator, splitStart: Long, absProbeRegionStart: Long, maxRecordLength: Long, absDataRegionEnd: Long, prober: Seekable => Boolean): Long = {
    // Set up absolute positions
    val absProbeRegionEnd = Math.min(absProbeRegionStart + maxRecordLength, absDataRegionEnd) // = splitStart + bufferLength
    val relProbeRegionEnd = Ints.checkedCast(absProbeRegionEnd - absProbeRegionStart)

    // System.err.println(s"absProbeRegionStart: $absProbeRegionStart - absProbeRegionEnd: $absProbeRegionEnd - relProbeRegionEnd: $relProbeRegionEnd")

    // Data region is up to the end of the buffer
    val relDataRegionEnd = absDataRegionEnd - absProbeRegionStart

    val seekable = nav.clone
    seekable.setPos(absProbeRegionStart - splitStart)
    seekable.limitNext(relDataRegionEnd)
    val charSequence = new CharSequenceFromSeekable(seekable)
    val fwdMatcher = trigFwdPattern.matcher(charSequence)
    fwdMatcher.region(0, relProbeRegionEnd)

    val matchPos = findPosition(seekable, fwdMatcher, true, prober)

    val result = if (matchPos >= 0) matchPos + splitStart else -1
    result
  }

  def readToBuffer(stream: InputStream, splitStart: Long, splitEnd: Long,
                   requestedExtraBytes: Int, stream2: InputStream): (ArrayBuffer[Byte], Int) = {
    val splitLength = splitEnd - splitStart

    // Get a buffer for the split + 1 mrl for finding the first record in the next split + extra bytes to perform
    // record validation in the next split
    // But also don't step over a complete split
    val buffer = new ArrayBuffer[Byte]()
    val length = 65 * 1024
    val arr = new Array[Byte](length)

    var n: Int = 0
    do {
      buffer ++= arr.slice(0, n)
      n = stream.read(arr, 0, length)
    } while (n >= 0)


    val arr2 = new Array[Byte](requestedExtraBytes)
    var actualExtraBytes = 0
    n = 0
    do {
      actualExtraBytes += n
      val remaining = requestedExtraBytes - actualExtraBytes
      n = if (remaining == 0) -1 else stream2.read(arr2, actualExtraBytes, remaining)
    } while (n >= 0)
    buffer ++= arr2.slice(0, actualExtraBytes)

    if (actualExtraBytes < 0) {
      throw new RuntimeException(s"Attempt to buffer $requestedExtraBytes bytes from split failed")
    }

    (buffer, actualExtraBytes)
  }

  def createDatasetFlowApproachEasyPeasy(splitStart: Long, splitEnd: Long, stream: InputStream, stream2: InputStream,
                                         context: TaskAttemptContext, pm: Model): Flowable[Dataset] = {

    val splitLength = splitEnd - splitStart

    // Get a buffer for the split + 1 mrl for finding the first record in the next split + extra bytes to perform
    // record validation in the next split
    // But also don't step over a complete split
    val rawDesiredBufferLength = splitLength + Math.min(2 * maxRecordLength + probeRecordCount * maxRecordLength, splitLength - 1)
    val desiredBufferLength = Ints.checkedCast(rawDesiredBufferLength)
    val (arr, extraLength) = readToBuffer(stream, splitStart, splitEnd, rawDesiredBufferLength)
    // System.err.println(s"Read $bufferLength bytes - requested: $desiredBufferLength")


    val dataRegionEnd = splitEnd + extraLength

    // System.err.println("Processing split " + splitStart + " - " + splitEnd + " | --+" + extraLength + "--> " + dataRegionEnd)

    val baos = new ByteArrayOutputStream()
    RDFDataMgr.write(baos, pm, RDFFormat.TURTLE_PRETTY)
    val prefixBytes = baos.toByteArray


    // Clones the provided seekable!
    val effectiveInputStreamSupp: Seekable => InputStream = seekable => {
      val r = new SequenceInputStream(
        new ByteArrayInputStream(prefixBytes),
        Channels.newInputStream(seekable.cloneObject))
      r
    }

    val parser: Seekable => Flowable[Dataset] = seekable => {
      // TODO Close the cloned seekable
      val task = new java.util.concurrent.Callable[InputStream]() {
        def call(): InputStream = effectiveInputStreamSupp.apply(seekable)
      }

      val r = RDFDataMgrRx.createFlowableDatasets(task, Lang.TRIG, null)
      r
    }

    val isNonEmptyDataset = new Predicate[Dataset] {
      override def test(t: Dataset): Boolean = {
        // System.err.println("Dataset filter saw graphs: " + t.listNames().asScala.toList)
        !t.isEmpty
      }
    }

    val prober: Seekable => Boolean = seekable => {
      val quadCount = parser(seekable)
        .limit(probeRecordCount)
        .count
        .onErrorReturnItem(-1L)
        .blockingGet() > 0
      quadCount
    }

    val buffer = ByteBuffer.wrap(arr)
    val pageManager = new PageManagerForByteBuffer(buffer)
    val nav = new PageNavigator(pageManager)

    nav.setPos(0L)


    // Find the second record in the next split - i.e. after splitEnd (inclusive)
    // This is to detect record parts that although cleanly separated by the split boundary still need to be aggregated,
    // such as <g> { } | <g> { }   (where '|' denotes the split boundary)
    var effectiveRecordRangeEnd = skipOverNextRecord(nav, splitStart, splitEnd, maxRecordLength, dataRegionEnd, prober)
    if(effectiveRecordRangeEnd < 0) {
      effectiveRecordRangeEnd = dataRegionEnd
    }

    // If we are at start 0, we parse from the beginning - otherwise we skip the first record
    val effectiveRecordRangeStart = if (splitStart == 0) {
      0L
    } else {
      skipOverNextRecord(nav, splitStart, splitStart, maxRecordLength, splitEnd, prober)
    }

    var result: Flowable[Dataset] = null
    if(effectiveRecordRangeStart >= 0) {
      val parseLength = effectiveRecordRangeEnd - effectiveRecordRangeStart
      nav.setPos(effectiveRecordRangeStart - splitStart)
      nav.limitNext(parseLength)
      result = parser(nav)
        // .onErrorReturnItem(EMPTY_DATASET)
        // .filter(isNonEmptyDataset)
    } else {
      result = Flowable.empty()
    }

    /*
    val cnt = result
      .count()
      .blockingGet()

    System.err.println("For effective region " + effectiveRecordRangeStart + " - " + effectiveRecordRangeEnd + " got " + cnt + " datasets")
    */

    result
  }

  override def nextKeyValue(): Boolean = {
    if (datasetFlow == null || !datasetFlow.hasNext) {
      // System.err.println("nextKeyValue: Drained all datasets from flow")
      false
    }
    else {
      currentValue = datasetFlow.next()
      // System.err.println("nextKeyValue: Got dataset value: " + currentValue.listNames().asScala.toList)
      // RDFDataMgr.write(System.err, currentValue, RDFFormat.TRIG_PRETTY)
      // System.err.println("nextKeyValue: Done printing out dataset value")
      currentKey.incrementAndGet
      currentValue != null
    }
  }

  override def getCurrentKey: LongWritable = if (currentValue == null) null else new LongWritable(currentKey.get)

  override def getCurrentValue: Dataset = currentValue

  override def getProgress: Float = 0

  override def close(): Unit = {
    if (datasetFlow != null) {
      datasetFlow = null
    }
  }
}
