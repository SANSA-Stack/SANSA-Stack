package net.sansa_stack.rdf.common.io.hadoop.trash;

import com.google.common.base.StandardSystemProperty;
import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.rdf.common.*;
import net.sansa_stack.rdf.common.io.hadoop.rdf.trig.RecordReaderTrigDataset;
import org.aksw.jena_sparql_api.io.binseach.BufferFromInputStream;
import org.aksw.jena_sparql_api.io.binseach.CharSequenceFromSeekable;
import org.aksw.jena_sparql_api.io.binseach.Seekable;
import org.aksw.jena_sparql_api.rx.RDFDataMgrRx;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.jena.ext.com.google.common.primitives.Ints;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * A technical readiness level 7+ (System prototype demonstration in operational environment)
 * record reader for Trig RDF files. Can handle data skews where
 * sometimes large data blocks span multiple splits. E.g. a mix of 1 million graphs of 1 triple
 * and 1 graph of 1 million triples.
 * <p>
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
public class TrigRecordReaderOld
        extends RecordReader<LongWritable, Dataset> {

    private static final Logger logger = LoggerFactory.getLogger(RecordReaderTrigDataset.class);

    public static String MAX_RECORD_LENGTH = "mapreduce.input.trigrecordreader.record.maxlength";
    public static String MIN_RECORD_LENGTH = "mapreduce.input.trigrecordreader.record.minlength";
    public static String PROBE_RECORD_COUNT = "mapreduce.input.trigrecordreader.probe.count";

    protected long maxRecordLength;
    protected long minRecordLength;
    protected int probeRecordCount;

    /**
     * Regex pattern to search for candidate record starts
     * used to avoid having to invoke the actual parser (which may start a new thread)
     * on each single character
     */
    protected static final Pattern trigFwdPattern = Pattern.compile("@?base|@?prefix|(graph\\s*)?(<[^>]*>|_?:[^-\\s]+)\\s*\\{", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    // private var start, end, position = 0L

    protected static final Dataset EMPTY_DATASET = DatasetFactory.create();

    protected AtomicLong currentKey = new AtomicLong();
    protected Dataset currentValue = DatasetFactory.create();

    protected Iterator<Dataset> datasetFlow;

    protected Decompressor decompressor;


    protected FileSplit split;
    protected CompressionCodec codec;
    protected byte[] prefixBytes;
    protected FSDataInputStream rawStream;
    protected SeekableInputStream stream;
    protected boolean isEncoded = false;
    protected long splitStart = -1;
    protected long splitLength = -1;
    protected long splitEnd = -1;


    /**
     * Read out config paramaters (prefixes, length thresholds, ...) and
     * examine the codec in order to set an internal
     * flag whether the stream will be encoded or not.
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        // println("TRIG READER INITIALIZE CALLED")
        Configuration job = context.getConfiguration();

        maxRecordLength = job.getInt(TrigRecordReaderOld.MAX_RECORD_LENGTH, 10 * 1024 * 1024);
        minRecordLength = job.getInt(TrigRecordReaderOld.MIN_RECORD_LENGTH, 1);
        probeRecordCount = job.getInt(TrigRecordReaderOld.PROBE_RECORD_COUNT, 10);

        String str = context.getConfiguration().get("prefixes");
        Model model = ModelFactory.createDefaultModel();
        if (str != null) RDFDataMgr.read(model, new StringReader(str), null, Lang.TURTLE);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RDFDataMgr.write(baos, model, RDFFormat.TURTLE_PRETTY);
        // val prefixBytes = baos.toByteArray
        prefixBytes = baos.toByteArray();


        split = (FileSplit) inputSplit;

        // By default use the given stream
        // We may need to wrap it with a decoder below
        // var stream: InputStream with fs.Seekable = split.getPath.getFileSystem(context.getConfiguration).open(split.getPath)
        org.apache.hadoop.fs.Path path = split.getPath();
        rawStream = path.getFileSystem(context.getConfiguration())
                .open(path);

//    if (rawStream.isInstanceOf[AbstractInterruptibleChannel]) {
//      doNotCloseOnInterrupt(rawStream.asInstanceOf[AbstractInterruptibleChannel])
//    }

        // println("Got raw stream positioned at: " + rawStream.getPos)
        isEncoded = false;


        // var splitStart = split.getStart
        // val splitLength = split.getLength
        // var splitEnd = splitStart + splitLength
        splitStart = split.getStart();
        splitLength = split.getLength();
        splitEnd = splitStart + splitLength;

        // maxRecordLength = Math.min(maxRecordLength, splitLength)
        // println("Split length = " + splitLength)

        // val rawDesiredBufferLength = split.getLength + Math.min(2 * maxRecordLength + probeRecordCount * maxRecordLength, split.getLength - 1)

        org.apache.hadoop.fs.Path file = split.getPath();

        codec = new CompressionCodecFactory(job).getCodec(file);
        // var streamFactory: Long => (InputStream, Long, Long) = null

        if (null != codec) {
            // decompressor = CodecPool.getDecompressor(codec)
            if (codec instanceof SplittableCompressionCodec) {
                // val scc = codec.asInstanceOf[SplittableCompressionCodec]
                isEncoded = true;
            } else {
                throw new RuntimeException("Don't know how to handle codec: " + codec);
            }
        }
    }

    public void initDatasetFlow() throws IOException {


        // val sw = Stopwatch.createStarted()
        // val (arr, extraLength) = readToBuffer(stream, isEncoded, splitStart, splitEnd, desiredExtraBytes)

        // println("TRIGREADER READ " + arr.length + " bytes (including " + desiredExtraBytes + " extra) in " + sw.elapsed(TimeUnit.MILLISECONDS) + " ms")

        Flowable<Dataset> tmp = createDatasetFlow();
        datasetFlow = tmp.blockingIterable().iterator();
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
    public Map.Entry<Long, Long> setStreamToInterval(long start, long end) throws IOException {

        Map.Entry<Long, Long> result;

        if (null != codec) {
            if (decompressor != null) {
                // CodecPool.returnDecompressor(decompressor)
            }

            decompressor = CodecPool.getDecompressor(codec);

            if (codec instanceof SplittableCompressionCodec) {
                SplittableCompressionCodec scc = (SplittableCompressionCodec) codec;

                // rawStream.seek(0)
                // rawStream.seek(start)
                // try {

                SplitCompressionInputStream tmp = scc.createInputStream(rawStream, decompressor, start, end,
                        SplittableCompressionCodec.READ_MODE.BYBLOCK);


//        val tmp: SplitCompressionInputStream = scc.createInputStream(
//          new SeekableInputStream(new CloseShieldInputStream(rawStream), rawStream), decompressor, start, end,
//          SplittableCompressionCodec.READ_MODE.BYBLOCK)

                // tmp.read(new Array[Byte](1))
                // tmp.skip(0)
                long adjustedStart = tmp.getAdjustedStart();
                long adjustedEnd = tmp.getAdjustedEnd();

                // val rawPos = rawStream.getPos
                // println(s"Adjusted: [$start, $end[ -> [$adjustedStart, $adjustedEnd[ - raw pos: $rawPos" )

                // stream = tmp // new SeekableInputStream(new CloseShieldInputStream(tmp), tmp)

                stream =
                        new SeekableInputStream(
                                new InputStreamWithCloseIgnore(
                                        InputStreamWithCloseLogging.wrap(tmp,
                                                ExceptionUtils::getStackTrace, RecordReaderTrigDataset::logUnexpectedClose)), tmp);

                result = new AbstractMap.SimpleEntry<>(adjustedStart, adjustedEnd);
                // } catch {
                // case _ => result = setStreamToInterval(start - 1, start -1)
                // }
            } else {
                throw new RuntimeException("Don't know how to handle codec: " + codec);
            }
        } else {
            rawStream.seek(start);
            // stream = rawStream
            // stream.seek(start)

            stream =
                    new SeekableInputStream(
                            new InputStreamWithCloseIgnore(
                                    Channels.newInputStream(
                                            new InterruptingReadableByteChannel(
                                                    InputStreamWithCloseLogging.wrap(rawStream,
                                                            ExceptionUtils::getStackTrace, RecordReaderTrigDataset::logUnexpectedClose), rawStream, end))),
                            rawStream);

            result = new AbstractMap.SimpleEntry<>(start, end);
        }

        return result;
    }

    public static void logClose(String str) {
        // logger.info(str)
    }

    public static void logUnexpectedClose(String str) {
        logger.error(str);
        throw new RuntimeException("Unexpected close");
    }

    protected Flowable<Dataset> createDatasetFlow() throws IOException {

        // System.err.println(s"Processing split $absSplitStart: $splitStart - $splitEnd | --+$actualExtraBytes--> $dataRegionEnd")

        // Clones the provided seekable!
        Function<Seekable, InputStream> effectiveInputStreamSupp = seekable -> {
            InputStream r = new SequenceInputStream(
                    new ByteArrayInputStream(prefixBytes),
                    Channels.newInputStream(seekable.cloneObject()));
            return r;
        };

        Function<Seekable, Flowable<Dataset>> parser = seekable -> {
            Callable<InputStream> inSupp = () -> effectiveInputStreamSupp.apply(seekable);

            Flowable<Dataset> r = RDFDataMgrRx.createFlowableDatasets(inSupp, Lang.TRIG, null);
            return r;
        };

        Predicate<Dataset> isNonEmptyDataset = t -> !t.isEmpty();

        Predicate<Seekable> prober = seekable -> {
            // printSeekable(seekable)
            boolean quadCount = parser.apply(seekable)
                    .take(probeRecordCount)
                    .count()
                    // .doOnError(new Consumer[Throwable] {
                    //   override def accept(t: Throwable): Unit = t.printStackTrace
                    // })

                    .onErrorReturnItem(-1L)
                    .blockingGet() > 0;
            return quadCount;
        };

        // Predicate to test whether a split position was hit or passed for the given stream
        // and position
        BiPredicate<SeekableInputStream, Long> hitSplitBound = (strm, splitPos) -> {
            long rawPos;
            try {
                rawPos = strm.getSeekable().getPos();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            long exceed = rawPos - splitPos;
            boolean eofReached = exceed >= 0;
            if (eofReached) {
                logger.warn("Exceeded maximum boundary by " + exceed + " bytes");
            }
            return eofReached;
        };


        // Except for the first split, the first record in each split is skipped
        // because it may belong to the last record of the previous split.
        // So we need to read past the first record, then find the second record
        // and then find probeRecordCount further records to validate the second one
        // Hence we need to read up to (2 + probeRecordCount) * maxRecordLength bytes
        long desiredExtraBytes = (2 + probeRecordCount) * maxRecordLength;

        // Set the stream to the end of the split and get the tail buffer
        Map.Entry<Long, Long> adjustedTailSplitBounds = setStreamToInterval(splitEnd, splitEnd + desiredExtraBytes);
        long adjustedSplitEnd = adjustedTailSplitBounds.getKey();

        BufferFromInputStream tailBuffer = BufferFromInputStream.create(new BoundedInputStream(stream, desiredExtraBytes), 1024 * 1024);
        Seekable tailNav = tailBuffer.newChannel();

        long tmp = skipOverNextRecord(tailNav, 0, 0, maxRecordLength, desiredExtraBytes, pos -> true, prober);
        long tailBytes = tmp < 0 ? 0 : Ints.checkedCast(tmp);

        // Set the stream to the start of the split and get the head buffer
        // Note that we will use the stream in its state to read the body part
        long knownDecodedDataLength[] = new long[]{-1};
        if (!isEncoded) {
            knownDecodedDataLength[0] = splitLength;
        }

        Map.Entry<Long, Long> adjustedHeadSplitBounds = setStreamToInterval(splitStart, adjustedSplitEnd);
        long adjustedSplitStart = adjustedHeadSplitBounds.getKey();

        Predicate<Long> posValidator = pos -> {
            boolean r = knownDecodedDataLength[0] < 0 || pos < knownDecodedDataLength[0];
            // logger.info("Validation request: " + pos + " known size=" + knownDecodedDataLength[0] + " valid=" + r);
            return r;
        };

        /*
        InputStream splitBoundedHeadStream =
                Channels.newInputStream(
                        new InterruptingReadableByteChannel(
                                stream,
                                rawStream,
                                adjustedSplitEnd,
                                self -> {
                                    long bytesRead = self.getBytesRead();
                                    if (knownDecodedDataLength[0] >= 0) {
                                        if (bytesRead == knownDecodedDataLength[0]) {
                                            throw new RuntimeException("Attempt to reset known data length to already known value: " + knownDecodedDataLength[0]);
                                        } else {
                                            throw new RuntimeException("Inconsistent decoded data length: " + knownDecodedDataLength[0] + " bytes were declared as known but split end reached after " + bytesRead + " bytes");
                                        }
                                    }

                                    knownDecodedDataLength[0] = bytesRead;
                                    logger.info("Head stream encountered split end; decoded data length = " + knownDecodedDataLength[0]);
                                }));

         */

        // In the following snippet note that ReadableByteChannelWithConditionalBound's
        // callback mechanism is only used to detect the split length of the decoded data.
        // It is NOT used to flag the end of the stream.
        InputStream splitBoundedHeadStream =
                Channels.newInputStream(
                        new ReadableByteChannelWithConditionalBound<ReadableByteChannel>(
                                new ReadableByteChannelWithoutCloseOnInterrupt(stream),
                                self -> {
                                    if (knownDecodedDataLength[0] < 0) {
                                        boolean foundBound = hitSplitBound.test(
                                                new SeekableInputStream(rawStream, rawStream),
                                                adjustedSplitEnd);

                                        if (foundBound) {
                                            knownDecodedDataLength[0] = self.getBytesRead();
                                            logger.info("Head stream encountered split end; decoded data length = " + knownDecodedDataLength[0]);
                                        }
                                    }

                                    // Never signal eof
                                    return false;
                                }));

//        InputStream splitBoundedHeadStream =
//                Channels.newInputStream(
//                        new ReadableByteChannelWithConditionalBound<ReadableByteChannel>(
//                                new ReadableByteChannelWithoutCloseOnInterrupt(stream),
//                                xstream -> hitSplitBound.test(new SeekableInputStream(rawStream, rawStream), adjustedSplitEnd)));

        BufferFromInputStream headBuffer = BufferFromInputStream.create(new BoundedInputStream(splitBoundedHeadStream, desiredExtraBytes), 1024 * 1024);
        Seekable headNav = headBuffer.newChannel();

        int headBytes = splitStart == 0
                ? 0
                : Ints.checkedCast(skipOverNextRecord(headNav, 0, 0, maxRecordLength, desiredExtraBytes, posValidator, prober));

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

        InputStream splitBoundedBodyStream =
                Channels.newInputStream(
                        new ReadableByteChannelWithConditionalBound<ReadableByteChannel>(
                                new ReadableByteChannelWithoutCloseOnInterrupt(stream),
                                xstream -> hitSplitBound.test(stream, adjustedSplitEnd)));

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
        InputStream prefixStream = new ByteArrayInputStream(prefixBytes);
        InputStream headStream = null;
        InputStream tailStream = null;

        if (headBytes < 0) {
            // FIXME There are two possibilities now why we couldn't find a record
            //  - There were errors in the data
            //  - There is a record that goes across this split
            logger.warn(String.format("No data found in split starting at %d. Possible reasons: " +
                    "(1) A large record spanning this split, " +
                    "(2) Syntax error(s) in the data," +
                    "(3) Some bug in the implementation", splitStart));

            // No data from this split
            headStream = new ByteArrayInputStream(new byte[0]);
            splitBoundedBodyStream = new ByteArrayInputStream(new byte[0]);
            tailStream = new ByteArrayInputStream(new byte[0]);
        } else {

            // headStream = new ByteArrayInputStream(headBuffer, headBytes, headBufferLength - headBytes)
            // We need to check the headLength here
            long headLength = knownDecodedDataLength[0] < 0
                    ? headBuffer.getKnownDataSize()
                    : knownDecodedDataLength[0];

            Seekable headChannel = headBuffer.newChannel();
            if (headBytes != 0) {
                // The method nextPos may read bytes from the head buffer if no bytes have been read yet!
                // So only call nextPos when we have to and know for sure we had prior reads. This is indicated by
                // a non-zero value of headBytes.
                headChannel.nextPos(headBytes);
            }

            headStream = new BoundedInputStream(Channels.newInputStream(headChannel), headLength - headBytes);

            // Sanity check for non-encoded data: The body must immediately follow
            // the (adjusted) split start + the know header size
            if (!isEncoded) {
                long expectedBodyOffset = adjustedSplitStart + headBuffer.getKnownDataSize();
                // println(s"adjustedSplitStart=$adjustedSplitStart + known head buffer size = ${headBuffer.getKnownDataSize} = $expectedBodyOffset - actual body offset = ${stream.getPos}")

                if (expectedBodyOffset != stream.getPos()) {
                    throw new RuntimeException("Expected body offset does not match actual one: adjustedSplitStart = " + adjustedSplitStart + " known head buffer size = " + headBuffer.getKnownDataSize() + ", expected body offset = " + expectedBodyOffset + ", actual body offset = " + stream.getPos());
                }
            }

            // Why the tailBuffer in encoded setting is displaced by 1 byte is beyond me...
            int displacement = isEncoded ? 1 : 0;

            Seekable tailChannel = tailBuffer.newChannel();
            tailChannel.nextPos(displacement);

            tailStream = new BoundedInputStream(Channels.newInputStream(tailChannel), tailBytes - displacement);
        }

        boolean writeOutSegments = false;

        if (writeOutSegments) {
            String splitName = split.getPath().getName();

            Path basePath = Paths.get(StandardSystemProperty.JAVA_IO_TMPDIR.value()).toAbsolutePath();

            logger.info("Writing segment " + splitName + " " + splitStart + " to " + basePath);

            // Path basePath = Paths.get("/mnt/LinuxData/tmp/");

            Path prefixFile = basePath.resolve(splitName + "_" + splitStart + ".prefix.trig");
            Path headFile = basePath.resolve(splitName + "_" + splitStart + ".head.trig");
            Path bodyFile = basePath.resolve(splitName + "_" + splitStart + ".body.trig");
            Path tailFile = basePath.resolve(splitName + "_" + splitStart + ".tail.trig");
            Files.copy(prefixStream, prefixFile);
            Files.copy(headStream, headFile);
            Files.copy(splitBoundedBodyStream, bodyFile);
            Files.copy(tailStream, tailFile);
            // Nicely close streams? Then again, must parts are in-memory buffers and this is debugging code only
            prefixStream = Files.newInputStream(prefixFile, StandardOpenOption.READ);
            headStream = Files.newInputStream(headFile, StandardOpenOption.READ);
            splitBoundedBodyStream = Files.newInputStream(bodyFile, StandardOpenOption.READ);
            tailStream = Files.newInputStream(tailFile, StandardOpenOption.READ);
        }


        InputStream fullStream = InputStreamWithCloseLogging.wrap(new SequenceInputStream(Collections.enumeration(
                Arrays.asList(prefixStream, headStream, splitBoundedBodyStream, tailStream))), ExceptionUtils::getStackTrace, RecordReaderTrigDataset::logClose);


        Flowable<Dataset> result = null;
        if (headBytes >= 0) {
            result = RDFDataMgrRx.createFlowableDatasets(() -> fullStream, Lang.TRIG, null);

            // val parseLength = effectiveRecordRangeEnd - effectiveRecordRangeStart
            // nav.setPos(effectiveRecordRangeStart - splitStart)
            // nav.limitNext(parseLength)
            // result = parser(nav)
            // .onErrorReturnItem(EMPTY_DATASET)
            // .filter(isNonEmptyDataset)
        } else {
            result = Flowable.empty();
        }

        //    val cnt = result
        //      .count()
        //      .blockingGet()
        //    System.err.println("For effective region " + effectiveRecordRangeStart + " - " + effectiveRecordRangeEnd + " got " + cnt + " datasets")

        return result;
    }


    /**
     * @param nav
     * @param splitStart
     * @param absProbeRegionStart
     * @param maxRecordLength
     * @param absDataRegionEnd
     * @param posValidator
     * @param prober
     * @return
     * @throws IOException
     */
    public static long findNextRecord(
            Seekable nav,
            long splitStart,
            long absProbeRegionStart,
            long maxRecordLength,
            long absDataRegionEnd,
            Predicate<Long> posValidator,
            Predicate<Seekable> prober) throws IOException {
        // Set up absolute positions
        long absProbeRegionEnd = Math.min(absProbeRegionStart + maxRecordLength, absDataRegionEnd); // = splitStart + bufferLength
        int relProbeRegionEnd = Ints.checkedCast(absProbeRegionEnd - absProbeRegionStart);

        // System.err.println(s"absProbeRegionStart: $absProbeRegionStart - absProbeRegionEnd: $absProbeRegionEnd - relProbeRegionEnd: $relProbeRegionEnd")

        // Data region is up to the end of the buffer
        long relDataRegionEnd = absDataRegionEnd - absProbeRegionStart;

        Seekable seekable = nav.cloneObject();
        seekable.setPos(absProbeRegionStart - splitStart);
        // seekable.limitNext(relDataRegionEnd)
        // val charSequence = new CharSequenceFromSeekable(seekable)
        // seekable.limitNext(relDataRegionEnd)
        // TODO The original code used limitNext but do we need that
        //  if we set the matcher region anyway?
        CharSequence charSequence = new CharSequenceFromSeekable(seekable);

        Matcher fwdMatcher = trigFwdPattern.matcher(charSequence);
        fwdMatcher.region(0, relProbeRegionEnd);

        long matchPos = findFirstPositionWithProbeSuccess(seekable, posValidator, fwdMatcher, true, prober);

        long result = matchPos >= 0
                ? matchPos + splitStart
                : -1;
        return result;
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
    long skipOverNextRecord(
            Seekable nav,
            long splitStart,
            long absProbeRegionStart,
            long maxRecordLength,
            long absDataRegionEnd,
            Predicate<Long> posValidator,
            Predicate<Seekable> prober) throws IOException {
        long result = -1L;

        long availableDataRegion = absDataRegionEnd - absProbeRegionStart;
        long nextProbePos = absProbeRegionStart;
        int i = 0;
        while (i < 2) {
            long candidatePos = findNextRecord(nav, splitStart, nextProbePos, maxRecordLength, absDataRegionEnd, posValidator, prober);
            if (candidatePos < 0) {
                // If there is more than maxRecordLength data available then
                // it is inconsistent for findNextRecord to indicate that no record was found
                // Either the maxRecordLength parameter is too small,
                // or there is an internal error with the prober
                // or there is a problem with the data (e.g. syntax error)
                if (availableDataRegion >= maxRecordLength) {
                    logger.warn("Found no record start in a record search region of " + maxRecordLength + " bytes, although up to " + availableDataRegion + " bytes were allowed for reading");
                    // throw new RuntimeException(s"Found no record start in a record search region of $maxRecordLength bytes, although $availableDataRegion bytes were available")
                } else {
                    // Here we assume we read into the last chunk which contained no full record
                    logger.warn("No more records found after pos " + (splitStart + nextProbePos));
                }


                // Retain best found candidate position
                // effectiveRecordRangeEnd = dataRegionEnd
                i = 666; // break
            } else {
                result = candidatePos;
                if (i == 0) {
                    nextProbePos = candidatePos + minRecordLength;
                }
                i += 1;
            }
        }

        return result;
    }

    /**
     * Uses the matcher to find candidate probing positions, and returns the first positoin
     * where probing succeeds.
     * Matching ranges are part of the matcher configuration
     *
     * @param rawSeekable
     * @param posValidator Test whether the seekable's absolute position is a valid start point.
     *                     Used to prevent testing start points past a split boundary with unknown split lengths.
     * @param m
     * @param isFwd
     * @param prober
     * @return
     */
    public static long findFirstPositionWithProbeSuccess(
            Seekable rawSeekable,
            Predicate<Long> posValidator,
            Matcher m,
            boolean isFwd,
            Predicate<Seekable> prober) throws IOException {

        Seekable seekable = rawSeekable.cloneObject();
        long absMatcherStartPos = seekable.getPos();

        long result = -1l;
        while (m.find()) {
            int start = m.start();
            int end = m.end();
            // The matcher yields absolute byte positions from the beginning of the byte sequence
            int matchPos = isFwd
                    ? start
                    : -end + 1;

            int absPos = Ints.checkedCast(absMatcherStartPos + matchPos);

            boolean validAbsPos = posValidator.test((long) absPos);
            if (!validAbsPos) {
                break;
            }

            // Artificially create errors
            // absPos += 5;
            seekable.setPos(absPos);
            Seekable probeSeek = seekable.cloneObject();

            boolean probeResult = prober.test(probeSeek);
            // System.err.println(s"Probe result for matching at pos $absPos with fwd=$isFwd: $probeResult")

            if (probeResult) {
                result = absPos;
                break;
            }
        }

        return result;
    }


    @Override
    public boolean nextKeyValue() throws IOException {
        if (datasetFlow == null) {
            initDatasetFlow();
        }

        boolean result;
        if (!datasetFlow.hasNext()) {
            // System.err.println("nextKeyValue: Drained all datasets from flow")
            result = false;
        } else {
            currentValue = datasetFlow.next();
            // System.err.println("nextKeyValue: Got dataset value: " + currentValue.listNames().asScala.toList)
            // RDFDataMgr.write(System.err, currentValue, RDFFormat.TRIG_PRETTY)
            // System.err.println("nextKeyValue: Done printing out dataset value")
            currentKey.incrementAndGet();
            result = currentValue != null;
        }

        return result;
    }

    @Override
    public LongWritable getCurrentKey() {
        LongWritable result = currentValue == null
                ? null
                : new LongWritable(currentKey.get());

        return result;
    }

    @Override
    public Dataset getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException {
        // Very raw estimate; does not take head / tail buffers into accoun
        // Also, in the beginning the position is beyond th split end in order to find the end position
        // At this time the progress would be reported as 100%
        float result = this.splitStart == this.splitEnd
                ? 0.0F
                : Math.min(1.0F, (this.rawStream.getPos() - this.splitStart) / (float) (this.splitEnd - this.splitStart));
        return result;
    }

    @Override
    public void close() throws IOException {
        try {
            // Stream apparently can be null if hadoop aborts a record reader early due
            // an encountered error (not necessarily related to this instance)
            if (rawStream != null) {
                // Under normal operation this close is redundant because the stream is owned
                // by the flowable which closes it the moment it has consumed the last item
                rawStream.close();
                rawStream = null;
            }
        } finally {
            if (this.decompressor != null) {
                CodecPool.returnDecompressor(this.decompressor);
                this.decompressor = null;
            }
        }
    }
}
