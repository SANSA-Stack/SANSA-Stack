package net.sansa_stack.rdf.common.io.hadoop

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util
import java.util.Collections
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Predicate
import java.util.regex.{Matcher, Pattern}

import io.reactivex.rxjava3.core.Flowable
import net.sansa_stack.rdf.common.{InterruptingReadableByteChannel, ReadableByteChannelWithConditionalBound, SeekableInputStream}
import org.aksw.jena_sparql_api.io.binseach._
import org.aksw.jena_sparql_api.rx.RDFDataMgrRx
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.fs
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.jena.ext.com.google.common.base.Stopwatch
import org.apache.jena.ext.com.google.common.primitives.Ints
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer


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

  private val logger = LoggerFactory.getLogger(classOf[TrigRecordReader])

  var maxRecordLength: Int = _
  var minRecordLength: Int = _
  var probeRecordCount: Int = _

  private val trigFwdPattern: Pattern = Pattern.compile("@?base|@?prefix|(graph\\s*)?(<[^>]*>|_?:[^-\\s]+)\\s*\\{", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE)

  // private var start, end, position = 0L

  private val EMPTY_DATASET: Dataset = DatasetFactory.create

  private val currentKey = new AtomicLong
  private var currentValue: Dataset = DatasetFactory.create()

  private var datasetFlow: util.Iterator[Dataset] = _

  protected var decompressor: Decompressor = _


  protected var codec: CompressionCodec = _
  protected var prefixBytes: Array[Byte] = _
  protected var rawStream: InputStream with fs.Seekable = _
  protected var stream: InputStream with fs.Seekable = _
  protected var isEncoded: Boolean = false
  protected var splitStart: Long = -1
  protected var splitLength: Long = -1
  protected var splitEnd: Long = -1


  /**
   * Read out config paramaters (prefixes, length thresholds, ...) and
   * examine the codec in order to set an internal
   * flag whether the stream will be encoded or not.
   *
   */
  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    // println("TRIG READER INITIALIZE CALLED")
    val job = context.getConfiguration

    maxRecordLength = job.getInt(TrigRecordReader.MAX_RECORD_LENGTH, 1 * 1024 * 1024)
    minRecordLength = job.getInt(TrigRecordReader.MIN_RECORD_LENGTH, 12)
    probeRecordCount = job.getInt(TrigRecordReader.PROBE_RECORD_COUNT, 5)

    val str = context.getConfiguration.get("prefixes")
    val model = ModelFactory.createDefaultModel()
    if (str != null) RDFDataMgr.read(model, new StringReader(str), null, Lang.TURTLE)

    val baos = new ByteArrayOutputStream()
    RDFDataMgr.write(baos, model, RDFFormat.TURTLE_PRETTY)
    // val prefixBytes = baos.toByteArray
    prefixBytes = baos.toByteArray


    val split = inputSplit.asInstanceOf[FileSplit]

    // By default use the given stream
    // We may need to wrap it with a decoder below
    // var stream: InputStream with fs.Seekable = split.getPath.getFileSystem(context.getConfiguration).open(split.getPath)
    rawStream = split.getPath.getFileSystem(context.getConfiguration).open(split.getPath)
    // println("Got raw stream positioned at: " + rawStream.getPos)
    isEncoded = false


    // var splitStart = split.getStart
    // val splitLength = split.getLength
    // var splitEnd = splitStart + splitLength
    splitStart = split.getStart
    splitLength = split.getLength
    splitEnd = splitStart + splitLength

    // maxRecordLength = Math.min(maxRecordLength, splitLength)
    // println("Split length = " + splitLength)

    // val rawDesiredBufferLength = split.getLength + Math.min(2 * maxRecordLength + probeRecordCount * maxRecordLength, split.getLength - 1)

    val file = split.getPath

    codec = new CompressionCodecFactory(job).getCodec(file)
    // var streamFactory: Long => (InputStream, Long, Long) = null

    if (null != codec) {
      // decompressor = CodecPool.getDecompressor(codec)
      if (codec.isInstanceOf[SplittableCompressionCodec]) {
        // val scc = codec.asInstanceOf[SplittableCompressionCodec]
        isEncoded = true
      } else {
        throw new RuntimeException("Don't know how to handle codec: " + codec)
      }
    }
  }

  def initDatasetFlow(): Unit = {


    val sw = Stopwatch.createStarted()
    // val (arr, extraLength) = readToBuffer(stream, isEncoded, splitStart, splitEnd, desiredExtraBytes)

    // println("TRIGREADER READ " + arr.length + " bytes (including " + desiredExtraBytes + " extra) in " + sw.elapsed(TimeUnit.MILLISECONDS) + " ms")

    val tmp = createDatasetFlow()

    datasetFlow = tmp.blockingIterable().iterator()
  }


  /**
   * Seek to a given offset and prepare to read up to the 'end' position (exclusive)
   * For non-encoded streams this is just performs a seek on th stream and returns
   * start/end unchanged.
   * Encoded streams will adjust the seek to the data part that follows some header
   * information
   *
   * @param start
   * @param end
   * @return
   */
  def setStreamToInterval(start: Long, end: Long): (Long, Long) = {

    var result: (Long, Long) = null

    if (null != codec) {
      val decompressor = CodecPool.getDecompressor(codec)

      if (codec.isInstanceOf[SplittableCompressionCodec]) {
        val scc = codec.asInstanceOf[SplittableCompressionCodec]

        // rawStream.seek(0)
        // rawStream.seek(start)
        // try {
          val tmp: SplitCompressionInputStream = scc.createInputStream(rawStream, decompressor, start, end,
            SplittableCompressionCodec.READ_MODE.BYBLOCK)

          // tmp.read(new Array[Byte](1))
          // tmp.skip(0)
          val adjustedStart = tmp.getAdjustedStart
          val adjustedEnd = tmp.getAdjustedEnd

          // val rawPos = rawStream.getPos
          // println(s"Adjusted: [$start, $end[ -> [$adjustedStart, $adjustedEnd[ - raw pos: $rawPos" )

          stream = tmp

          result = (adjustedStart, adjustedEnd)
        // } catch {
        // case _ => result = setStreamToInterval(start - 1, start -1)
        // }
      } else {
        throw new RuntimeException("Don't know how to handle codec: " + codec)
      }
    } else {
      rawStream.seek(start)
      // stream = rawStream
      // stream.seek(start)
      stream = new SeekableInputStream(
        Channels.newInputStream(new InterruptingReadableByteChannel(rawStream, rawStream, end)),
        rawStream);

      result = (start, end)
    }

    result
  }


  def createDatasetFlow(): Flowable[Dataset] = {

    // System.err.println(s"Processing split $absSplitStart: $splitStart - $splitEnd | --+$actualExtraBytes--> $dataRegionEnd")

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
      // printSeekable(seekable)
      val quadCount = parser(seekable)
        .take(probeRecordCount)
        .count
        // .doOnError(new Consumer[Throwable] {
        //   override def accept(t: Throwable): Unit = t.printStackTrace
        // })
        .onErrorReturnItem(-1L)
        .blockingGet() > 0
      quadCount
    }

    // Except for the first split, the first record in each split is skipped
    // because it may belong to the last record of the previous split.
    // So we need to read past the first record, then find the second record
    // and then find probeRecordCount further records to validate the second one
    // Hence we need to read up to (2 + probeRecordCount) * maxRecordLength bytes
    val desiredExtraBytes = (2 + probeRecordCount) * maxRecordLength
    /*
    Ints.checkedCast(Math.min(
      2 * maxRecordLength + probeRecordCount * maxRecordLength,
      splitLength - 1))
    }
    */

    // logger.info("desiredExtraBytes = " + desiredExtraBytes)


    // FIXME these buffers can become very large when dealing with huge graphs
    // especially when their content stretch over splits
    // Somehow find a way to allocate them dynamically
    // val tailBuffer: Array[Byte] = new Array[Byte](desiredExtraBytes)
    // val headBuffer: Array[Byte] = new Array[Byte](desiredExtraBytes)

    // val inputSplit: InputSplit = null


    // Set the stream to the end of the split and get the tail buffer
    val (adjustedSplitEnd, _) = setStreamToInterval(splitEnd, splitEnd + desiredExtraBytes)
    // val (adjustedSplitEnd, _) = setStreamToInterval(splitEnd, splitEnd + splitLength)

    if (adjustedSplitEnd != splitEnd) {
      throw new RuntimeException("difference!")
    }

    // val deltaSplitEnd = adjustedSplitEnd - splitEnd
    // println(s"Adjusted split end $splitEnd to $adjustedSplitEnd [adjusted by $deltaSplitEnd bytes]")

    // val tailBufferLength = IOUtils.read(stream, tailBuffer, 0, tailBuffer.length)
    // val tailBufferLength = readAvailable(stream, tailBuffer, 0, tailBuffer.length)

    val tailBuffer = BufferFromInputStream.create(new BoundedInputStream(stream, desiredExtraBytes), 1024 * 1024)
    val tailNav: Seekable = tailBuffer.newChannel()

    // val buffer = BufferFromInputStream.create(stream, 1024 * 1024).newChannel()




    // val tailNav = new PageNavigator(new PageManagerForByteBuffer(ByteBuffer.wrap(tailBuffer)))
    // val tmp = skipOverNextRecord(tailNav, 0, 0, maxRecordLength, tailBufferLength, prober)
    val tmp = skipOverNextRecord(tailNav, 0, 0, maxRecordLength, desiredExtraBytes, prober)
    val tailBytes = if (tmp < 0) 0 else Ints.checkedCast(tmp)

    // val tailBufferLength = fuckRead2(stream, tailBuffer, 0, tailBuffer.length, rawStream, adjustedSplitEnd)
    // println("Raw stream position [" + Thread.currentThread() + "]: " + stream.getPos)

    // Set the stream to the start of the split and get the head buffer
    // Note that we will use the stream in its state to read the body part

    val (adjustedSplitStart, _) = setStreamToInterval(splitStart, adjustedSplitEnd)
    // val (adjustedSplitStart, _) = setStreamToInterval(splitStart, splitEnd)

    // TODO We need to ensure the headBuffer content does not go beyond the split boundary
    // othewise it results in an overlap with the tail buffer
    // val headBufferLength = IOUtils.read(stream, headBuffer, 0, headBuffer.length)

    // val headChannelFactory = BufferFromInputStream.create(stream, 1024 * 1024)

    // TODO We need to dynamically cap the read at the split boundary
    // Probably we just ned to modify the InterruptingReadablyByteChannel to return EOF
    // val headBufferLength = readAvailableByBlock(stream, headBuffer, 0, headBuffer.length, rawStream, adjustedSplitEnd)

    val hitSplitBound: (InputStream with fs.Seekable, Long) => Boolean = (strm, splitPos) => {
      val rawPos = strm.getPos

      val exceed = rawPos - splitPos
      val eofReached = exceed >= 0
      if (eofReached) {
        logger.warn("Exceeded maximum boundary by " + exceed + " bytes")
      }
      eofReached
    }

    val splitBoundedHeadStream = Channels.newInputStream(new ReadableByteChannelWithConditionalBound[ReadableByteChannel](Channels.newChannel(stream),
      xstream => hitSplitBound(stream, adjustedSplitEnd)))


    val headBuffer = BufferFromInputStream.create(new BoundedInputStream(splitBoundedHeadStream, desiredExtraBytes), 1024 * 1024)
    val headNav: Seekable = headBuffer.newChannel()


    val headBytes: Int = if (splitStart == 0) {
      0
    } else {

      // val headNav = new PageNavigator(new PageManagerForByteBuffer(ByteBuffer.wrap(headBuffer)))
      // Ints.checkedCast(skipOverNextRecord(headNav, 0, 0, maxRecordLength, headBufferLength, prober))
      Ints.checkedCast(skipOverNextRecord(headNav, 0, 0, maxRecordLength, desiredExtraBytes, prober))
    }

    // println("Raw stream position [" + Thread.currentThread() + "]: " + stream.getPos)

    //    val deltaSplitStart = adjustedSplitStart - splitStart
    // println(s"Adjusted split start $splitStart to $adjustedSplitStart [$deltaSplitStart]")

    // Stream is now positioned at beginning of body region
    // And head and tail buffers have been populated

    logger.info(s"adjustment: [$splitStart, $splitEnd) -> [$adjustedSplitStart, $adjustedSplitEnd)")
    // logger.info(s"[head: $headBufferLength] [ $splitLength ] [$tailBufferLength]")

    // Set up the body stream whose read method returns
    // -1 upon reaching the split boundry
    var splitBoundedBodyStream: InputStream =
      Channels.newInputStream(new ReadableByteChannelWithConditionalBound[ReadableByteChannel](Channels.newChannel(stream),
      xstream => hitSplitBound(stream, adjustedSplitEnd)))

/*
    var bodyStream: InputStream = Channels.newInputStream(new ReadableByteChannel {
      val blockBuffer: Array[Byte] = new Array[Byte](1 * 1024 * 1024)
      var lastRead = -1

      override def read(dst: ByteBuffer): Int = {
        var n: Int = 0
        val streamPos = stream.getPos
        val remainingSplitLen = Ints.saturatedCast(adjustedSplitEnd - streamPos)
        if (remainingSplitLen <= 0) {
          // println(s"Remaining splitlen $remainingSplitLen - $streamPos / $splitEnd / $adjustedSplitEnd")
          // println(s"LAST BUFFER [size=$lastRead]: " + new String(blockBuffer, 0, lastRead, StandardCharsets.UTF_8))

          n = -1
        } else {
          val remainingBufferLen = dst.remaining()
          // If the stream is encoded we do not know how many bytes we need to read
          // and rely on the read to return on the encoding block boundary
          //          val readLimit = if (isEncoded) remainingBufferLen
          //            else Math.min(remainingBufferLen, remainingSplitLen)
          val readLimit = Math.min(Math.min(remainingBufferLen, remainingSplitLen), blockBuffer.length)

          // TODO We could read into dst directly
          n = stream.read(blockBuffer, 0, readLimit)

          // println(s"read limit = $readLimit - n = $n")
          if (n >= 0) {
            lastRead = n
            dst.put(blockBuffer, 0, n)
          }
          // else {
          // println(s"End of stream reached; lastRead=$lastRead")
          // }
        }
        n
      }

      override def isOpen: Boolean = true

      override def close(): Unit = {}
    })
*/

    // Find the second record in the next split - i.e. after splitEnd (inclusive)
    // This is to detect record parts that although cleanly separated by the split boundary still need to be aggregated,
    // such as <g> { } | <g> { }   (where '|' denotes the split boundary)

    // If we are at start 0, we parse from the beginning - otherwise we skip the first record

    // println("HEAD BUFFER: " + new String(headBuffer, headBytes, headBufferLength - headBytes, StandardCharsets.UTF_8))
    // println("TAIL BUFFER: " + new String(tailBuffer, 0, tailBytes, StandardCharsets.UTF_8))

    // Assemble the overall stream
    var prefixStream: InputStream = new ByteArrayInputStream(prefixBytes)

    var headStream: InputStream = null
    // var bodyStream: InputStream = null
    var tailStream: InputStream = null

    if (headBytes < 0) {
      logger.error("NEED TO PROPERLY HANDLE THE CASE WHERE NO RECORD FOUND IN HEAD")

      // No data from this split
      headStream = new ByteArrayInputStream(Array[Byte]())
      splitBoundedBodyStream = new ByteArrayInputStream(Array[Byte]())
      tailStream = new ByteArrayInputStream(Array[Byte]())
    } else {


      // headStream = new ByteArrayInputStream(headBuffer, headBytes, headBufferLength - headBytes)

      val headChannel = headBuffer.newChannel()
      headChannel.nextPos(headBytes)
      headStream = new BoundedInputStream(Channels.newInputStream(headChannel), headBuffer.getKnownDataSize - headBytes)


      // Why the tailBuffer in encoded setting is displaced by 1 byte is beyond me...
      val displacement = if (isEncoded) 1 else 0
      // val displacement = 0

      // tailStream = new ByteArrayInputStream(tailBuffer, displacement, tailBytes - displacement)
      val tailChannel = tailBuffer.newChannel()
      tailChannel.nextPos(displacement)
      /*
      for (i <- 0 to 10) {
        val pos = tailChannel.getPos()
        val ch = tailChannel.get(i)
        println(s"i - pos: ${pos} STRING: ${ch}")
      }
      System.exit(0)
      */
      tailStream = new BoundedInputStream(Channels.newInputStream(tailChannel), tailBytes - displacement)
      // tailStream = new BoundedInputStream(Channels.newInputStream(tailBuffer.newChannel()), tailBytes - displacement)
      // val tailStream = new ByteArrayInputStream(tailBuffer, 0, tailBytes)
    }

    val writeOutSegments = false

    if (writeOutSegments) {
      logger.info("Writing segment " + splitStart)

      val prefixFile = Paths.get("/tmp/segment" + splitStart + ".prefix.trig")
      val headFile = Paths.get("/tmp/segment" + splitStart + ".head.trig")
      val bodyFile = Paths.get("/tmp/segment" + splitStart + ".body.trig")
      val tailFile = Paths.get("/tmp/segment" + splitStart + ".tail.trig")
      Files.copy(prefixStream, prefixFile)
      Files.copy(headStream, headFile)
      Files.copy(splitBoundedBodyStream, bodyFile)
      Files.copy(tailStream, tailFile)
      // Nicely close streams? Then again, must parts are in-memory buffers and this is debugging code only
      prefixStream = Files.newInputStream(prefixFile, StandardOpenOption.READ)
      headStream = Files.newInputStream(headFile, StandardOpenOption.READ)
      splitBoundedBodyStream = Files.newInputStream(bodyFile, StandardOpenOption.READ)
      tailStream = Files.newInputStream(tailFile, StandardOpenOption.READ)
    }


    var fullStream: InputStream = new SequenceInputStream(Collections.enumeration(
      util.Arrays.asList(prefixStream, headStream, splitBoundedBodyStream, tailStream)))



    var result: Flowable[Dataset] = null
    if(headBytes >= 0) {
      result = RDFDataMgrRx.createFlowableDatasets(new Callable[InputStream] {
        override def call(): InputStream = fullStream
      }, Lang.TRIG, null)

      // val parseLength = effectiveRecordRangeEnd - effectiveRecordRangeStart
      // nav.setPos(effectiveRecordRangeStart - splitStart)
      // nav.limitNext(parseLength)
      // result = parser(nav)
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


  def findNextRecord(nav: Seekable, splitStart: Long, absProbeRegionStart: Long, maxRecordLength: Long, absDataRegionEnd: Long, prober: Seekable => Boolean): Long = {
    // Set up absolute positions
    val absProbeRegionEnd = Math.min(absProbeRegionStart + maxRecordLength, absDataRegionEnd) // = splitStart + bufferLength
    val relProbeRegionEnd = Ints.checkedCast(absProbeRegionEnd - absProbeRegionStart)

    // System.err.println(s"absProbeRegionStart: $absProbeRegionStart - absProbeRegionEnd: $absProbeRegionEnd - relProbeRegionEnd: $relProbeRegionEnd")

    // Data region is up to the end of the buffer
    val relDataRegionEnd = absDataRegionEnd - absProbeRegionStart

    val seekable: Seekable = nav.cloneObject
    seekable.setPos(absProbeRegionStart - splitStart)
    // seekable.limitNext(relDataRegionEnd)
    // val charSequence = new CharSequenceFromSeekable(seekable)
    // seekable.limitNext(relDataRegionEnd)
    // TODO The original code used limitNext but do we need that
    //  if we set the matcher region anyway?
    val charSequence = new CharSequenceFromSeekable(seekable)

    val fwdMatcher = trigFwdPattern.matcher(charSequence)
    fwdMatcher.region(0, relProbeRegionEnd)

    val matchPos = findFirstPositionWithProbeSuccess(seekable, fwdMatcher, true, prober)

    val result = if (matchPos >= 0) matchPos + splitStart else -1
    result
  }

  /**
   * Find the start of the *second* recard as seen from 'splitStart' (inclusive)
   *
   *
   * @param nav
   * @param splitStart
   * @param absProbeRegionStart
   * @param maxRecordLength
   * @param absDataRegionEnd
   * @param prober
   * @return
   */
  def skipOverNextRecord(
                          nav: Seekable,
                          splitStart: Long,
                          absProbeRegionStart: Long,
                          maxRecordLength: Long,
                          absDataRegionEnd: Long,
                          prober: Seekable => Boolean): Long = {
    var result = -1L

    val availableDataRegion = absDataRegionEnd - absProbeRegionStart
    var nextProbePos = absProbeRegionStart
    var i = 0
    while (i < 2) {
      val candidatePos = findNextRecord(nav, splitStart, nextProbePos, maxRecordLength, absDataRegionEnd, prober)
      if (candidatePos < 0) {
        // If there is more than maxRecordLength data available then
        // it is inconsistent for findNextRecord to indicate that no record was found
        // Either the maxRecordLength parameter is too small,
        // or there is an internal error with the prober
        // or there is a problem with the data (e.g. syntax error)
        if (availableDataRegion >= maxRecordLength) {
          // FIXME
          logger.warn(s"TODO AVAILABLE DATA IS NOT YET ADJUSTED TO ACTUAL DATA. Found no record start in a record search region of $maxRecordLength bytes, although $availableDataRegion bytes were available")
          // throw new RuntimeException(s"Found no record start in a record search region of $maxRecordLength bytes, although $availableDataRegion bytes were available")
        } else {
          // Here we assume we read into the last chunk which contained no full record
          logger.warn("No more records found after pos " + (splitStart + nextProbePos))
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
  def findFirstPositionWithProbeSuccess(rawSeekable: Seekable, m: Matcher, isFwd: Boolean, prober: Seekable => Boolean): Long = {

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


  override def nextKeyValue(): Boolean = {
    if (datasetFlow == null) {
      initDatasetFlow()
    }

    if (!datasetFlow.hasNext) {
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
  }




  /**
   * Read available data - until an underlying backing stream reaches a boundary
   * assumes that the underlying stream is conceptually in hadoop's READ_BY_BLOCK mode
   * i.e. read() returns upon reaching the boundary with the position set to that boundary
   */
  @throws[IOException]
  def readAvailableByBlock(input: InputStream, buffer: Array[Byte], offset: Int, length: Int,
                           rawStream: InputStream with fs.Seekable, maxRawPos: Long): Int = {

    if (length < 0) throw new IllegalArgumentException("Length must not be negative: " + length)

    if (offset + length > buffer.length) {
      throw new IllegalArgumentException("Requested offset + length greater than capacity of the provided buffer")
    }

    // Workaround for https://issues.apache.org/jira/browse/HADOOP-17453:
    // Using non-zero offsets for read are bugged
    // So we read into this intermediate workaround buffer before writing to the actual output buffer
    val workaroundBuffer = new Array[Byte](1024 * 1024)

    var remaining = length
    var result = 0
    var iter = 0
    while (remaining > 0) {
      iter += 1
      var contrib = 0
      val nextOffset = offset + result

      val cap = Math.min(workaroundBuffer.length, remaining)
      contrib = input.read(workaroundBuffer, 0, cap)
        // val beforeReadPos = input.asInstanceOf[fs.Seekable].getPos
        // contrib = input.read(buffer, nextOffset, remaining)
        // println("[" + Thread.currentThread() + "]: Attempt to read from pos " + pos + " with contrib " + contrib + " lead to " + input.asInstanceOf[fs.Seekable].getPos)
//      } catch {
// //        case e: IndexOutOfBoundsException =>
// //          println("[" + Thread.currentThread() + "]: IndexOutOfBounds ignored")
// //          contrib = -1
//        case e => throw new RuntimeException("error", e)
//      }
      // println("COUNT [" + Thread.currentThread() + "]: contributed " + contrib + " bytes in iteration " + iter)
      if (contrib < 0) {
        remaining = 0
      } else {
        System.arraycopy(workaroundBuffer, 0, buffer, nextOffset, contrib)

        result += contrib
        remaining -= contrib
      }

      val rawPos = rawStream.getPos

      val exceed = rawPos - maxRawPos
      if (exceed >= 0) {
        logger.warn("Exceeded maximum boundary by " + exceed + " bytes")
        remaining = 0
      }

    }
    result
  }


  @throws[IOException]
  def readAvailable(input: InputStream, buffer: Array[Byte], offset: Int, length: Int): Int = {

    // Workaround for https://issues.apache.org/jira/browse/HADOOP-17453:
    // Using non-zero offsets for read are bugged
    // So we read into this intermediate workaround buffer before writing to the actual output buffer
    val workaroundBuffer = new Array[Byte](1024 * 1024)

    if (length < 0) throw new IllegalArgumentException("Length must not be negative: " + length)

    if (offset + length > buffer.length) {
      throw new IllegalArgumentException("Requested offset + length greater than capacity of the provided buffer")
    }

    var remaining = length
    var result = 0
    var iter = 0
    while (remaining > 0) {
      iter += 1
      var contrib = 0
      val nextOffset = offset + result
//      try {

      // val pos = input.asInstanceOf[fs.Seekable].getPos
      val cap = Math.min(workaroundBuffer.length, remaining)
      contrib = input.read(workaroundBuffer, 0, cap)
        // println("[" + Thread.currentThread() + "]: Attempt to read from pos " + pos + " with contrib " + contrib + " lead to " + input.asInstanceOf[fs.Seekable].getPos)
//      } catch {
//        // case e: IndexOutOfBoundsException =>
//        //  println("[" + Thread.currentThread() + "]: IndexOutOfBounds ignored")
//        //  contrib = -1
//        case e => throw new RuntimeException("error", e)
//      }
      // println("COUNT [" + Thread.currentThread() + "]: contributed " + contrib + " bytes in iteration " + iter)
      if (contrib < 0) { // EOF

        // Adjust the result to EOF if we haven't seen any byts
        // if (result == 0) {
        //   result = -1
        // }
        remaining = 0
      } else {
        System.arraycopy(workaroundBuffer, 0, buffer, nextOffset, contrib)

        result += contrib
        remaining -= contrib
      }
    }
    result
  }

  // def printSeekable(seekable: Seekable): Unit = {
  //   val tmp = seekable.cloneObject()
  //   val pos = seekable.getPos
  //   System.out.println(s"BUFFER: $pos" + IOUtils.toString(Channels.newInputStream(tmp)))
  // }




  // This pattern is no longer needed and its not up to date
  // private val trigBwdPattern: Pattern = Pattern.compile("esab@?|xiferp@?|\\{\\s*(>[^<]*<|[^-\\s]+:_)\\s*(hparg)?", Pattern.CASE_INSENSITIVE)

  /**
   * DEPRECATED AND NO LONGER USED! Our current approach does much better than reading large chunks into
   * buffers - kept here for reference for the time being but will likely be removed
   *
   * Transfers all decoded data that corresponds to the region
   * between splitStart and splitEnd of the given stream
   * plus a given number of extra bytes into a buffer
   *
   * @param stream The input stream, possibly compressed
   * @param splitStart Start position of the split which may be encoded data
   * @param splitEnd End position of the split which may be encoded data
   * @param requestedExtraBytes Additional number of decoded bytes to read
   * @return
   */
  def readToBuffer(stream: InputStream with fs.Seekable, isEncoded: Boolean, splitStart: Long, splitEnd: Long,
                   requestedExtraBytes: Int): (ArrayBuffer[Byte], Int) = {

    val splitLength = splitEnd - splitStart

    // TODO ArrayBuffer has linear complexity for appending; use a better data structure
    val buffer = new ArrayBuffer[Byte]()
    // Read data in blocks of 'length' size
    // It is important to understand that the
    // stream's read method by contract must return once it hits a block boundary
    // This does not hold for non-encoded streams for which we count the bytes ourself
    val length = 1 * 1024 * 1024
    val blockBuffer = new Array[Byte](length)

    var n: Int = 0
    do {
      buffer ++= blockBuffer.slice(0, n)

      val streamPos = stream.getPos
      if (streamPos >= splitEnd) {
        n = -1
      } else {
        val readLimit = if (isEncoded) length else Math.min(length, Ints.checkedCast(splitLength - buffer.length))

        n = stream.read(blockBuffer, 0, readLimit)
      }
    } while (n >= 0)


    val tailBuffer = new Array[Byte](requestedExtraBytes)
    var actualExtraBytes = 0
    n = 0
    do {
      actualExtraBytes += n
      val remaining = requestedExtraBytes - actualExtraBytes
      n = if (remaining == 0) -1 else stream.read(tailBuffer, actualExtraBytes, remaining)
    } while (n >= 0)
    buffer ++= tailBuffer.slice(0, actualExtraBytes)

    if (actualExtraBytes < 0) {
      throw new RuntimeException(s"Attempt to buffer $requestedExtraBytes bytes from split failed")
    }

    (buffer, actualExtraBytes)
  }
}


/*
  def readToBuffer2(stream: InputStream with fs.Seekable, buffer: Array[Byte], initialOffset: Int, len: Int): Int = {

    val startPos = stream.getPos
    val splitEnd = startPos + len

    var offset = initialOffset
    var n: Int = 0
    do {
      val remaining = Math.min(buffer.length - offset, len)
      if(remaining > 0) {
        val streamPos = stream.getPos
        n = stream.read(buffer, offset, remaining)

        if(n >= 0) {
          offset += n
        }
      } else {
        n = -1
      }
    } while (n >= 0)


    offset - initialOffset
  }
  */


