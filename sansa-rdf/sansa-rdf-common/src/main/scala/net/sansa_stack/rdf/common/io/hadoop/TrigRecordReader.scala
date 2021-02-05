package net.sansa_stack.rdf.common.io.hadoop

import com.google.common.io.ByteStreams

import java.io._
import java.nio.channels.{Channels, ReadableByteChannel}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util
import java.util.Collections
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Predicate
import java.util.regex.{Matcher, Pattern}
import io.reactivex.rxjava3.core.Flowable
import net.sansa_stack.rdf.common.{InputStreamWithCloseIgnore, InputStreamWithCloseLogging, InterruptingReadableByteChannel, ReadableByteChannelImpl2, ReadableByteChannelWithConditionalBound, SeekableInputStream}
import org.aksw.jena_sparql_api.io.binseach._
import org.aksw.jena_sparql_api.rx.RDFDataMgrRx
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.{BoundedInputStream, CloseShieldInputStream}
import org.apache.commons.lang3.exception.ExceptionUtils
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


/**
 * A technical readiness level 7+ (System prototype demonstration in operational environment)
 * record reader for Trig RDF files. Can handle data skews where
 * sometimes large data blocks span multiple splits. E.g. a mix of 1 million graphs of 1 triple
 * and 1 graph of 1 million triples.
 *
 * Each split is separated into a head, body and tail region.
 * <ul>
 *   <li>
 *     The <b>tail</b> region <b>always<b/> extends beyond the current split's end up to the <b>starting position</b>
 *     of the <b>second</b> record in the successor split. The first record of the successor split may actually
 *     be a continuation of a record on this split: If you condsider two quads separated by the split
 *     boundary such as ":g :s :p :o |splitboundary| :g :x :y :z" then the first record after the boundary
 *     still uses the graph :g and thus belongs to the graph record started in the current split.
 *   </li>
 *   <li>Likewise, the head region always - with one exception - starts at the <i>second</i> record in a split (because as mentioned, the first record may
 *   belong to the prior split. <b>Unless</b> it is the first split (identified by an absolute starting
 *   position of 0. Then the head region start at the beginning of the split).
 *   The length of the head region depends on the number of data was considered for verifying that the starting
 *   position is a valid record start.
 *   </li>
 *   <li>The body region immediately starts after the head region and extends up to the split boundary</li>
 *   <li>We first buffer the tail region and then the head region. As a consequence, after buffering the head
 *   region the underlying stream is positioned at the start of the body region</li>
 *   <li>The effective input stream comprises the buffer of the head region, the 'live' stream of
 *   the body region (up to the split boundary) following by the tail region. This is then passed to the
 *   RDF parser which for valid input data is then able to parse all records in a single pass without
 *   errors or interruptions.
 *   </li>
 * </ul>
 *
 * @author Claus Stadler
 * @author Lorenz Buehmann
 */
object TrigRecordReader {
  val MAX_RECORD_LENGTH = "mapreduce.input.trigrecordreader.record.maxlength"
  val MIN_RECORD_LENGTH = "mapreduce.input.trigrecordreader.record.minlength"
  val PROBE_RECORD_COUNT = "mapreduce.input.trigrecordreader.probe.count"
}
class TrigRecordReader
  extends RecordReader[LongWritable, Dataset] {

  private val logger = LoggerFactory.getLogger(classOf[TrigRecordReader])

  var maxRecordLength: Long = _
  var minRecordLength: Long = _
  var probeRecordCount: Int = _

  /**
   * Regex pattern to search for candidate record starts
   * used to avoid having to invoke the actual parser (which may start a new thread)
   * on each single character
   */
  private val trigFwdPattern: Pattern = Pattern.compile("@?base|@?prefix|(graph\\s*)?(<[^>]*>|_?:[^-\\s]+)\\s*\\{", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE)

  // private var start, end, position = 0L

  private val EMPTY_DATASET: Dataset = DatasetFactory.create

  private val currentKey = new AtomicLong
  private var currentValue: Dataset = DatasetFactory.create()

  private var datasetFlow: util.Iterator[Dataset] = _

  protected var decompressor: Decompressor = _


  protected var split: FileSplit = _
  protected var codec: CompressionCodec = _
  protected var prefixBytes: Array[Byte] = _
  protected var rawStream: InputStream with fs.Seekable = _
  protected var stream: InputStream with fs.Seekable = _
  protected var isEncoded: Boolean = false
  protected var splitStart: Long = -1
  protected var splitLength: Long = -1
  protected var splitEnd: Long = -1


  import sun.nio.ch.Interruptible
  import java.lang.reflect.Field
  import java.nio.channels.FileChannel
  import java.nio.channels.spi.AbstractInterruptibleChannel

  /**
   * It took one week to figure out the race condition that would indeterministically cause jena to bail out with
   * parse errors in the assembled head/body/tail buffers. So the reason is, that RDFDataMgrRx tries
   * to interrupt the producer thread of the RDF parser (if there is one). However, FileChannel
   * extends AbstractInterruptibleChannel which in turn closes itself if during read the thread is interrupted.
   *
   * https://stackoverflow.com/questions/52261086/is-there-a-way-to-prevent-closedbyinterruptexception
   */
  def doNotCloseOnInterrupt(fc: AbstractInterruptibleChannel): Unit = {
    try {
      val field = classOf[AbstractInterruptibleChannel].getDeclaredField("interruptor")
      field.setAccessible(true)
      field.set(fc, (thread: Thread) => logger.warn(fc + " not closed on interrupt").asInstanceOf[Interruptible])
    } catch {
      case e: Exception =>
        logger.warn("Couldn't disable close on interrupt", e)
    }
  }
  /**
   * Read out config paramaters (prefixes, length thresholds, ...) and
   * examine the codec in order to set an internal
   * flag whether the stream will be encoded or not.
   *
   */
  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    // println("TRIG READER INITIALIZE CALLED")
    val job = context.getConfiguration

    maxRecordLength = job.getInt(TrigRecordReader.MAX_RECORD_LENGTH, 10 * 1024 * 1024)
    minRecordLength = job.getInt(TrigRecordReader.MIN_RECORD_LENGTH, 1)
    probeRecordCount = job.getInt(TrigRecordReader.PROBE_RECORD_COUNT, 10)

    val str = context.getConfiguration.get("prefixes")
    val model = ModelFactory.createDefaultModel()
    if (str != null) RDFDataMgr.read(model, new StringReader(str), null, Lang.TURTLE)

    val baos = new ByteArrayOutputStream()
    RDFDataMgr.write(baos, model, RDFFormat.TURTLE_PRETTY)
    // val prefixBytes = baos.toByteArray
    prefixBytes = baos.toByteArray


    split = inputSplit.asInstanceOf[FileSplit]

    // By default use the given stream
    // We may need to wrap it with a decoder below
    // var stream: InputStream with fs.Seekable = split.getPath.getFileSystem(context.getConfiguration).open(split.getPath)
    rawStream = split.getPath.getFileSystem(context.getConfiguration).open(split.getPath)

//    if (rawStream.isInstanceOf[AbstractInterruptibleChannel]) {
//      doNotCloseOnInterrupt(rawStream.asInstanceOf[AbstractInterruptibleChannel])
//    }

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


    // val sw = Stopwatch.createStarted()
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
      if (decompressor != null) {
        // CodecPool.returnDecompressor(decompressor)
      }

      decompressor = CodecPool.getDecompressor(codec)

      if (codec.isInstanceOf[SplittableCompressionCodec]) {
        val scc = codec.asInstanceOf[SplittableCompressionCodec]

        // rawStream.seek(0)
        // rawStream.seek(start)
        // try {

        val tmp: SplitCompressionInputStream = scc.createInputStream(rawStream, decompressor, start, end,
          SplittableCompressionCodec.READ_MODE.BYBLOCK)


//        val tmp: SplitCompressionInputStream = scc.createInputStream(
//          new SeekableInputStream(new CloseShieldInputStream(rawStream), rawStream), decompressor, start, end,
//          SplittableCompressionCodec.READ_MODE.BYBLOCK)

        // tmp.read(new Array[Byte](1))
        // tmp.skip(0)
        val adjustedStart = tmp.getAdjustedStart
        val adjustedEnd = tmp.getAdjustedEnd

        // val rawPos = rawStream.getPos
        // println(s"Adjusted: [$start, $end[ -> [$adjustedStart, $adjustedEnd[ - raw pos: $rawPos" )

        // stream = tmp // new SeekableInputStream(new CloseShieldInputStream(tmp), tmp)

        stream =
          new SeekableInputStream(
            new InputStreamWithCloseIgnore(
              InputStreamWithCloseLogging.wrap(tmp,
                ExceptionUtils.getStackTrace(_), logUnexpectedClose(_))), tmp)

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

      stream =
        new SeekableInputStream(
          new InputStreamWithCloseIgnore(
            Channels.newInputStream(
              new InterruptingReadableByteChannel(
                InputStreamWithCloseLogging.wrap(rawStream,
                  ExceptionUtils.getStackTrace(_), logUnexpectedClose(_)), rawStream, end))),
        rawStream);

      result = (start, end)
    }

    result
  }

  def logClose(str: String): Unit = {
    // logger.info(str)
  }
  def logUnexpectedClose(str: String): Unit = {
    logger.error(str)
    throw new RuntimeException("Unexpected close")
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

    // Predicate to test whether a split position was hit or passed for the given stream
    // and position
    val hitSplitBound: (InputStream with fs.Seekable, Long) => Boolean = (strm, splitPos) => {
      val rawPos = strm.getPos

      val exceed = rawPos - splitPos
      val eofReached = exceed >= 0
      if (eofReached) {
        logger.warn("Exceeded maximum boundary by " + exceed + " bytes")
      }
      eofReached
    }


    // Except for the first split, the first record in each split is skipped
    // because it may belong to the last record of the previous split.
    // So we need to read past the first record, then find the second record
    // and then find probeRecordCount further records to validate the second one
    // Hence we need to read up to (2 + probeRecordCount) * maxRecordLength bytes
    val desiredExtraBytes: Long = (2 + probeRecordCount) * maxRecordLength

    // Set the stream to the end of the split and get the tail buffer
    val (adjustedSplitEnd, _) = setStreamToInterval(splitEnd, splitEnd + desiredExtraBytes)

    val tailBuffer = BufferFromInputStream.create(new BoundedInputStream(stream, desiredExtraBytes), 1024 * 1024)
    val tailNav: Seekable = tailBuffer.newChannel()

    val tmp = skipOverNextRecord(tailNav, 0, 0, maxRecordLength, desiredExtraBytes, prober)
    val tailBytes = if (tmp < 0) 0 else Ints.checkedCast(tmp)

    // Set the stream to the start of the split and get the head buffer
    // Note that we will use the stream in its state to read the body part
    val (adjustedSplitStart, _) = setStreamToInterval(splitStart, adjustedSplitEnd)

    val splitBoundedHeadStream =
      Channels.newInputStream(
        new ReadableByteChannelWithConditionalBound[ReadableByteChannel](
          new ReadableByteChannelImpl2(stream),
      xstream => hitSplitBound(rawStream, adjustedSplitEnd)))

    val headBuffer = BufferFromInputStream.create(new BoundedInputStream(splitBoundedHeadStream, desiredExtraBytes), 1024 * 1024)
    val headNav: Seekable = headBuffer.newChannel()

    val headBytes: Int = if (splitStart == 0) {
      0
    } else {
      Ints.checkedCast(skipOverNextRecord(headNav, 0, 0, maxRecordLength, desiredExtraBytes, prober))
    }

    // println("Raw stream position [" + Thread.currentThread() + "]: " + stream.getPos)

    //    val deltaSplitStart = adjustedSplitStart - splitStart
    // println(s"Adjusted split start $splitStart to $adjustedSplitStart [$deltaSplitStart]")

    // Stream is now positioned at beginning of body region
    // And head and tail buffers have been populated
    // logger.info(s"adjustment: [$splitStart, $splitEnd) -> [$adjustedSplitStart, $adjustedSplitEnd)")
    // logger.info(s"[head: $headBufferLength] [ $splitLength ] [$tailBufferLength]")

    // Set up the body stream whose read method returns
    // -1 upon reaching the split boundry

//    var splitBoundedBodyStream: InputStream = InputStreamWithCloseLogging.wrap(
//    new CloseShieldInputStream(Channels.newInputStream(new ReadableByteChannelWithConditionalBound[ReadableByteChannel](Channels.newChannel(stream),
//      xstream => hitSplitBound(stream, adjustedSplitEnd)))), ExceptionUtils.getStackTrace(_), logger.info(_))

    var splitBoundedBodyStream: InputStream =
      Channels.newInputStream(
        new ReadableByteChannelWithConditionalBound[ReadableByteChannel](
          new ReadableByteChannelImpl2(stream),
      xstream => hitSplitBound(stream, adjustedSplitEnd)))

    /*
        val bodyCore = InputStreamWithCloseLogging.wrap(
          Channels.newInputStream(new ReadableByteChannelWithConditionalBound[ReadableByteChannel](Channels.newChannel(stream),
            xstream => hitSplitBound(rawStream, adjustedSplitEnd))), ExceptionUtils.getStackTrace(_), logClose(_))

        var splitBoundedBodyStream: InputStream = new CloseShieldInputStream(bodyCore)

        val bodyBytes = ByteStreams.toByteArray(splitBoundedBodyStream)

        if (bodyBytes.length == 0) {
          logger.warn(s"Original split start/end: $splitStart - $splitEnd - pos: ${rawStream.getPos}")
          logger.warn(s"Adjusted split start/end: $adjustedSplitStart - $adjustedSplitEnd - pos: ${rawStream.getPos}")
          logger.warn(s"Head bytes: $headBytes - known head buffer size:" + headBuffer.getKnownDataSize)

          logger.warn("0 length body - WTF IS WRONG???")

          val again = ByteStreams.toByteArray(bodyCore)
          logger.warn("Now got: " + again.length)
        }

        splitBoundedBodyStream = new ByteArrayInputStream(bodyBytes)
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
    var tailStream: InputStream = null

    if (headBytes < 0) {
      // FIXME There are two possibilities now why we couldn't find a record
      //  - There were errors in the data
      //  - There is a record that goes across this split
      logger.warn("NEED TO PROPERLY HANDLE THE CASE WHERE NO RECORD FOUND IN HEAD")

      // No data from this split
      headStream = new ByteArrayInputStream(Array[Byte]())
      splitBoundedBodyStream = new ByteArrayInputStream(Array[Byte]())
      tailStream = new ByteArrayInputStream(Array[Byte]())
    } else {


      // headStream = new ByteArrayInputStream(headBuffer, headBytes, headBufferLength - headBytes)

      val headChannel = headBuffer.newChannel()
      headChannel.nextPos(headBytes)
      headStream = new BoundedInputStream(Channels.newInputStream(headChannel), headBuffer.getKnownDataSize - headBytes)

      // Sanity check for non-encoded data: The body must immediately follow
      // the (adjusted) split start + the know header size
      if (!isEncoded) {
        val expectedBodyOffset = adjustedSplitStart + headBuffer.getKnownDataSize
        // println(s"adjustedSplitStart=$adjustedSplitStart + known head buffer size = ${headBuffer.getKnownDataSize} = $expectedBodyOffset - actual body offset = ${stream.getPos}")

        if (expectedBodyOffset != stream.getPos) {
          throw new RuntimeException("Expected body offset does not match actual one: adjustedSplitStart=$adjustedSplitStart + known head buffer size = ${headBuffer.getKnownDataSize} = $expectedBodyOffset - actual body offset = ${stream.getPos}")
        }
      }

      // Why the tailBuffer in encoded setting is displaced by 1 byte is beyond me...
      val displacement = if (isEncoded) 1 else 0

      val tailChannel = tailBuffer.newChannel()
      tailChannel.nextPos(displacement)

      tailStream = new BoundedInputStream(Channels.newInputStream(tailChannel), tailBytes - displacement)
    }

    val writeOutSegments = false

    if (writeOutSegments) {
      val splitName = split.getPath.getName

      logger.info("Writing segment " + splitName + " " + splitStart)

      val basePath = Paths.get("/mnt/LinuxData/tmp/")

      val prefixFile = basePath.resolve(splitName + "_" + splitStart + ".prefix.trig")
      val headFile = basePath.resolve(splitName + "_" + splitStart + ".head.trig")
      val bodyFile = basePath.resolve(splitName + "_" + splitStart + ".body.trig")
      val tailFile = basePath.resolve(splitName + "_" + splitStart + ".tail.trig")
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


    var fullStream: InputStream = InputStreamWithCloseLogging.wrap(new SequenceInputStream(Collections.enumeration(
      util.Arrays.asList(prefixStream, headStream, splitBoundedBodyStream, tailStream))), ExceptionUtils.getStackTrace(_), logClose(_))


    var result: Flowable[Dataset] = null
    if (headBytes >= 0) {
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

    //    val cnt = result
    //      .count()
    //      .blockingGet()
    //    System.err.println("For effective region " + effectiveRecordRangeStart + " - " + effectiveRecordRangeEnd + " got " + cnt + " datasets")

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

      if (probeResult) {
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

  override def getProgress: Float = {
    // Very raw estimate; does not take head / tail buffers into accoun
    // Also, in the beginning the position is beyond th split end in order to find the end position
    // At this time the progress would be reported as 100%
    if (this.splitStart == this.splitEnd) 0.0F
    else Math.min(1.0F, (this.rawStream.getPos - this.splitStart).toFloat / (this.splitEnd - this.splitStart).toFloat)
  }

  override def close(): Unit = {
    try {
      // Stream apparently can be null if hadoop aborts a record reader early due
      // an encountered error (not necessarily related to this instance)
      if (rawStream != null) {
        // Under normal operation this close is redundant because the stream is owned
        // by the flowable which closes it the moment it has consumed the last item
        rawStream.close
        rawStream = null
      }
    } finally {
      if (this.decompressor != null) {
        CodecPool.returnDecompressor(this.decompressor)
        this.decompressor = null
      }
    }
  }
}
