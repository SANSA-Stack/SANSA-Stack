package net.sansa_stack.hadoop.core;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.core.pattern.CustomMatcher;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.util.InputStreamWithCloseLogging;
import net.sansa_stack.hadoop.util.SeekableByteChannelFromSeekableInputStream;
import net.sansa_stack.hadoop.util.SeekableInputStream;
import net.sansa_stack.io.util.InputStreamWithCloseIgnore;
import net.sansa_stack.io.util.InputStreamWithZeroOffsetRead;
import net.sansa_stack.nio.util.InterruptingSeekableByteChannel;
import net.sansa_stack.nio.util.ReadableByteChannelFromInputStream;
import net.sansa_stack.nio.util.ReadableByteChannelWithConditionalBound;
import org.aksw.commons.io.seekable.api.Seekable;
import org.aksw.commons.util.stream.SequentialGroupBySpec;
import org.aksw.commons.util.stream.StreamOperatorSequentialGroupBy;
import org.aksw.jena_sparql_api.io.binseach.BufferOverInputStream;
import org.aksw.jena_sparql_api.io.binseach.CharSequenceFromSeekable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.jena.ext.com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A generic record reader that uses a callback mechanism to detect a consecutive sequence of records
 * that must start in the current split and which may extend over any number of successor splits.
 * The callback that needs to be implemented is {@link #parse(InputStream)}.
 * This method receives a supplier of input streams and must return an RxJava {@link Flowable} over such an
 * input stream.
 * Note that in constrast to Java Streams, RxJava Flowables idiomatically
 * support error propagation, cancellation and resource management.
 * The RecordReaderGenericBase framework will attempt to read a configurable number of records from the
 * flowable in order to decide whether a position on the input stream is a valid start of records.
 * If for a probe position the flowable yields an error or zero records then probing continues at a higher
 * offset until the end of data (or the MAX_RECORD_SIZE limit) is reached.
 *
 * This implementation can therefore handle data skews where
 * sometimes large data blocks span multiple splits. E.g. a mix of 1 million graphs of 1 triple
 * and 1 graph of 1 million triples.
 * <p>
 * Each split is separated into a head, body and tail region.
 * <ul>
 *   <li>
 *     The <b>tail</b> region <b>always<b/> extends beyond the current split's end up to the <b>starting position</b>
 *     of the <b>third</b> record in the successor split. The first record of the successor split may actually
 *     be a continuation of a record on this split: If you condsider two quads separated by the split
 *     boundary such as ":g :s :p :o |splitboundary| :g :x :y :z" then the first record after the boundary
 *     still uses the graph :g and thus belongs to the graph record started in the current split.
 *   </li>
 *   <li>Likewise, the head region always - with one exception - starts at the <i>third</i> record in a split (because as mentioned, the first record may
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
public abstract class RecordReaderGenericBase<U, G, A, T>
        extends RecordReader<LongWritable, T> // TODO use CombineFileInputFormat?
{
    private static final Logger logger = LoggerFactory.getLogger(RecordReaderGenericBase.class);

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    protected final String minRecordLengthKey;
    protected final String maxRecordLengthKey;
    protected final String probeRecordCountKey;
    // protected final String headerBytesKey;

    /**
     * Regex pattern to search for candidate record starts
     * used to avoid having to invoke the actual parser (which may start a new thread)
     * on each single character
     */
    protected CustomPattern recordStartPattern;

    protected final Accumulating<U, G, A, T> accumulating;

    protected long maxRecordLength;
    protected long minRecordLength;
    protected int probeRecordCount;


    // private var start, end, position = 0L

    // protected static final Dataset EMPTY_DATASET = DatasetFactory.create();

    protected AtomicLong currentKey = new AtomicLong();
    protected T currentValue; // = DatasetFactory.create();

    protected Runnable recordFlowCloseable;
    protected Iterator<T> datasetFlow;

    protected Decompressor decompressor;

    protected FileSplit split;
    protected CompressionCodec codec;


    /**
     * Subclasses may initialize the pre/post-amble bytes in the
     * {@link #initialize(InputSplit, TaskAttemptContext)} method rather
     * than the ctor!
     *
     * A (possibly empty) sequence of bytes to prepended to any stream passed to the parser.
     * For example, for RDF data this could be a set of prefix declarations.
     */
    protected byte[] preambleBytes = EMPTY_BYTE_ARRAY;
    protected byte[] postambleBytes = EMPTY_BYTE_ARRAY;

    protected FSDataInputStream rawStream;
    protected SeekableInputStream stream;
    protected boolean isEncoded = false;
    protected long splitStart = -1;
    protected long splitLength = -1;
    protected long splitEnd = -1;
    protected boolean isFirstSplit = false;


    // Will be set in case an error occurs
    protected Throwable raisedThrowable = null;

    public RecordReaderGenericBase(
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            CustomPattern recordStartPattern,
            // String headerBytesKey,
            Accumulating<U, G, A, T> accumulating) {
        this.minRecordLengthKey = minRecordLengthKey;
        this.maxRecordLengthKey = maxRecordLengthKey;
        this.probeRecordCountKey = probeRecordCountKey;
        this.recordStartPattern = recordStartPattern;
        // this.headerBytesKey = headerBytesKey;
        this.accumulating = accumulating;
    }

    /**
     * Read out config paramaters (prefixes, length thresholds, ...) and
     * examine the codec in order to set an internal
     * flag whether the stream will be encoded or not.
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        // println("TRIG READER INITIALIZE CALLED")
        Configuration job = context.getConfiguration();

        minRecordLength = job.getInt(minRecordLengthKey, 1);
        maxRecordLength = job.getInt(maxRecordLengthKey, 10 * 1024 * 1024);
        probeRecordCount = job.getInt(probeRecordCountKey, 100);

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

        isFirstSplit = splitStart == 0;

        // maxRecordLength = Math.min(maxRecordLength, splitLength)
        // println("Split length = " + splitLength)

        // val rawDesiredBufferLength = split.getLength + Math.min(2 * maxRecordLength + probeRecordCount * maxRecordLength, split.getLength - 1)

        org.apache.hadoop.fs.Path file = split.getPath();

        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(job);
        codec = compressionCodecFactory.getCodec(file);

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

    public void initRecordFlow() throws IOException {
        // val sw = Stopwatch.createStarted()
        // val (arr, extraLength) = readToBuffer(stream, isEncoded, splitStart, splitEnd, desiredExtraBytes)
        // println("TRIGREADER READ " + arr.length + " bytes (including " + desiredExtraBytes + " extra) in " + sw.elapsed(TimeUnit.MILLISECONDS) + " ms")

        Stream<T> tmp = createRecordFlow();
        recordFlowCloseable = tmp::close;
        datasetFlow = tmp.iterator();
    }

    /**
     * Create a flowable from the input stream.
     * The input stream may be incorrectly positioned in which case the Flowable
     * is expected to indicate this by raising an error event.
     *
     * @param inputStream A supplier of input streams. May supply the same underlying stream
     *                    on each call hence only at most a single stream should be taken from the supplier.
     *                    Supplied streams are safe to use in try-with-resources blocks (possibly using CloseShieldInputStream).
     *                    Taken streams should be closed by the client code.
     * @return
     */
    protected abstract Stream<U> parse(InputStream inputStream);

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
                // Not sure if returning a decompressor resets its state properly in all cases
                // may want to comment the line out
                // CodecPool.returnDecompressor(decompressor);
            }

            decompressor = codec.createDecompressor(); // CodecPool.getDecompressor(codec);

            if (codec instanceof SplittableCompressionCodec) {
                SplittableCompressionCodec scc = (SplittableCompressionCodec) codec;

                // System.out.println(String.format("Setting stream to start %d - end %d", start, end));
                SplitCompressionInputStream tmp = scc.createInputStream(rawStream, decompressor, start, end,
                        SplittableCompressionCodec.READ_MODE.BYBLOCK);

                long adjustedStart = tmp.getAdjustedStart();
                long adjustedEnd = tmp.getAdjustedEnd();

                stream =
                        new SeekableInputStream(
                                new InputStreamWithCloseIgnore(InputStreamWithCloseLogging.wrap(
                                        new InputStreamWithZeroOffsetRead(tmp),
                                        ExceptionUtils::getStackTrace, RecordReaderGenericBase::logUnexpectedClose)),
                                tmp);

                result = new AbstractMap.SimpleEntry<>(adjustedStart, adjustedEnd);
            } else {
                // TODO Add support for non-splittable codecs: If the codec is non-splittable then we
                //   just get a split across the whole file
                //   and we just have to wrap it with the codec for decoding - so actually its easy...
                throw new RuntimeException("Don't know how to handle codec: " + codec);
            }
        } else {
            rawStream.seek(start);

            stream =
                    new SeekableInputStream(
                            new InputStreamWithCloseIgnore(
                                    Channels.newInputStream(
                                            new InterruptingSeekableByteChannel(
                                                    new SeekableByteChannelFromSeekableInputStream(
                                                        InputStreamWithCloseLogging.wrap(rawStream,
                                                                ExceptionUtils::getStackTrace, RecordReaderGenericBase::logUnexpectedClose),
                                                        rawStream),
                                                    end))),
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

    // Predicate to test whether a split position was hit or passed for the given stream
    // and position
    public boolean didHitSplitBound(org.apache.hadoop.fs.Seekable seekable, long splitPos) {
        long rawPos;
        try {
            rawPos = seekable.getPos();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long exceededSplitPos = rawPos - splitPos;
        boolean virtualEofReached = exceededSplitPos >= 0;
        if (virtualEofReached) {
            int maxExpectedExcess = isEncoded ? 1 : 0;

            if (exceededSplitPos > maxExpectedExcess) {
                logger.warn("Exceeded split pos by " + exceededSplitPos + " bytes");
            }
        }
        return virtualEofReached;
    }


    /**
     * Modify a flow to perform aggregation of items
     * into records according to specification
     * The complex part here is to correctly combine the two flows:
     *  - The first group of the splitAggregateFlow needs to be skipped as this in handled by the previous split's processor
     *  - If there are no further groups in splitFlow then no items are emitted at all (because all items belong to s previous split)
     *  - ONLY if the splitFlow owned at least one group: The first group in the tailFlow needs to be emitted
     *
     * @param isFirstSplit If true then the first record is included in the output; otherwise it is skipped
     * @param splitFlow The flow of items obtained from the split
     * @param tailItems The first set of group items after in the next split
     * @return
     */
    protected Stream<T> aggregate(boolean isFirstSplit, Stream<U> splitFlow, List<U> tailItems) {
        // We need to be careful to not return null as a flow item:
        // FlowableOperatorSequentialGroupBy returns a stream of (key, accumulator) pairs
        // Returning a null accumulator is ok as long it resides within the pair
        SequentialGroupBySpec<U, G, A> spec = SequentialGroupBySpec.create(
                accumulating::classify,
                (accNum, groupKey) -> !isFirstSplit && accNum == 0
                        ? null
                        : accumulating.createAccumulator(groupKey),
                accumulating::accumulate);

        Stream<T> result = StreamOperatorSequentialGroupBy.create(spec)
                .transform(Stream.concat(splitFlow, tailItems.stream()))
                .map(e -> accumulating.accumulatedValue(e.getValue()));
        if (!isFirstSplit) {
            result = result.skip(1);
        }
        // Stream<T> result = entryStream.map(e -> accumulating.accumulatedValue(e.getValue()));
        return result;
    }

    protected InputStream effectiveInputStream(InputStream base) {
        return base;
    }


    protected InputStream effectiveInputStreamSupp(Seekable seekable) {
        InputStream r = new SequenceInputStream(
                new ByteArrayInputStream(preambleBytes),
                effectiveInputStream(Channels.newInputStream(seekable.cloneObject())));
        return r;
    }

    public static long getPos(Seekable seekable) {
        long result;
        try {
            result = seekable.getPos();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    protected Stream<U> parseFromSeekable(Seekable seekable) {
        InputStream in = effectiveInputStreamSupp(seekable);
        Stream<U> r = parse(in);
        return r;
    }

    protected boolean prober(Seekable seekable) {
        // long pos = getPos(seekable);

        // printSeekable(seekable)
        long recordCount;
        try (Stream<U> stream = parseFromSeekable(seekable)
                    .limit(probeRecordCount)) {
                recordCount = stream.count();
        } catch (Throwable e) {
            recordCount = -1;
        }
                    // .collect(Collectors.toCollection(() -> new ArrayList<>(probeRecordCount)));

        boolean foundValidRecordOffset = recordCount > 0;

        // System.out.println(String.format("Probing at pos %s: %b", pos, foundValidRecordOffset));
        return foundValidRecordOffset;
    }

    protected Stream<T> createRecordFlow() throws IOException {

        String splitName = split.getPath().getName();
        String splitId = splitName + ":" + splitStart + "+" + splitLength;

        // System.err.println(s"Processing split $absSplitStart: $splitStart - $splitEnd | --+$actualExtraBytes--> $dataRegionEnd")

        // Clones the provided seekable!




        // Except for the first split, the first record may parse sucessfully but may
        // actually be an incomplete record that is cut off an the split boundary and that would
        // thus be interpreted incorrectly. For examyle, a quad "<g> | <s> <p> <o>" with "|" indicating the
        // split boundary would be incorrectly seen as a triple in the default graph.
        // The second record may be wrongly connected to the incomplete record, so the third record should be safe.
        // Once we find the start of the third record we
        // then need to find probeRecordCount further records to validate the the starting position.
        // Hence we need to read up to (3 + probeRecordCount) * maxRecordLength bytes
        int skipRecordCount = 2;
        long desiredExtraBytes = (skipRecordCount + probeRecordCount) * maxRecordLength;

        // Set the stream to the end of the split and get the tail buffer
        Map.Entry<Long, Long> adjustedTailSplitBounds = setStreamToInterval(splitEnd, splitEnd + desiredExtraBytes);
        long adjustedSplitEnd = adjustedTailSplitBounds.getKey();

        BufferOverInputStream tailBuffer = BufferOverInputStream.create(new BoundedInputStream(stream, desiredExtraBytes), 1024 * 1024);
        Seekable tailNav = tailBuffer.newChannel();


        StopWatch tailSw = StopWatch.createStarted();
        long tailRecordOffset = skipToNthRecord(skipRecordCount, tailNav, 0, 0, maxRecordLength, desiredExtraBytes, pos -> true, this::prober);
        // If no record is found in the tail then take all its known bytes because
        // we assume we hit the last few splits of the stream and there simply are no further record
        // starts anymore
        long tailBytes = tailRecordOffset < 0 ? tailBuffer.getKnownDataSize() : Ints.checkedCast(tailRecordOffset);

        // Now that we found an offset in the tail region, read out one more complete list of items that belongs to one group
        // Note, that these items may need to be aggregated with items from the current split - that's why we retain them as a list
        tailNav.setPos(tailBytes);

        // lines(tailNav).limit(100).forEach(System.out::println);

        SequentialGroupBySpec<U, G, List<U>> spec = SequentialGroupBySpec.create(
                accumulating::classify, () -> (List<U>)new ArrayList(), Collection::add);

        List<U> tailItems = StreamOperatorSequentialGroupBy.create(spec)
                .transform(parseFromSeekable(tailNav))
                .map(Map.Entry::getValue)
                .findFirst().orElse(Collections.emptyList());

        long tailItemTime = tailSw.getTime(TimeUnit.MILLISECONDS);
        logger.info(String.format("In split %s got %d tail items at pos %d within %d bytes read in %d ms",
                splitId,
                tailItems.size(),
                adjustedSplitEnd,
                tailBytes,
                tailItemTime));

        // Set the stream to the start of the split and get the head buffer
        // Note that we will use the stream later in its state to read the body part
        long knownDecodedDataLength[] = new long[]{ isEncoded ? -1 : splitLength };

        Map.Entry<Long, Long> adjustedHeadSplitBounds = setStreamToInterval(splitStart, adjustedSplitEnd);
        long adjustedSplitStart = adjustedHeadSplitBounds.getKey();

        Predicate<Long> posValidator = pos -> {
            boolean r = knownDecodedDataLength[0] < 0 || pos < knownDecodedDataLength[0];
            // logger.info("Validation request: " + pos + " known size=" + knownDecodedDataLength[0] + " valid=" + r);
            return r;
        };


        // In the following snippet note that ReadableByteChannelWithConditionalBound's
        // callback mechanism is only used to detect the split length of the decoded data.
        // It is NOT used to flag the end of the stream.
        InputStream splitBoundedHeadStream =
                Channels.newInputStream(
                        new ReadableByteChannelWithConditionalBound<ReadableByteChannel>(
                                new ReadableByteChannelFromInputStream(stream),
                                self -> {
                                    if (knownDecodedDataLength[0] < 0) {
                                        boolean foundBound = didHitSplitBound(
                                                stream,
                                                adjustedSplitEnd);

                                        if (foundBound) {
                                            knownDecodedDataLength[0] = self.getBytesRead();
                                            logger.info("Head stream encountered split end; decoded data length = " + knownDecodedDataLength[0]);
                                        }
                                    }

                                    // Never signal eof
                                    return false;
                                }));

        BufferOverInputStream headBuffer = BufferOverInputStream.create(new BoundedInputStream(splitBoundedHeadStream, desiredExtraBytes), 1024 * 1024);
        Seekable headNav = headBuffer.newChannel();

        StopWatch headSw = StopWatch.createStarted();

        int headBytes = isFirstSplit
                ? 0
                : Ints.checkedCast(skipToNthRecord(skipRecordCount, headNav, 0, 0, maxRecordLength, desiredExtraBytes, posValidator, this::prober));

        long headRecordTime = headSw.getTime(TimeUnit.MILLISECONDS);
        logger.info(String.format("In split %s found head record at pos %d with %d bytes read in %d ms",
                splitId,
                headBytes,
                headBuffer.getKnownDataSize(),
                headRecordTime));


        // println("Raw stream position [" + Thread.currentThread() + "]: " + stream.getPos)

        // long deltaSplitStart = adjustedSplitStart - splitStart;
        // logger.debug(String.format("Adjusted split start %d to %d [%d]", splitStart, adjustedSplitStart, deltaSplitStart);

        // Stream is now positioned at beginning of body region
        // And head and tail buffers have been populated
        logger.debug(String.format("Adjusted [%d, %d) to [%d, %d) | delta: (%d, %d)",
                splitStart, splitEnd,
                adjustedSplitStart, adjustedSplitEnd,
                adjustedSplitStart - splitStart,
                adjustedSplitEnd - splitEnd));
        // logger.info(s"[head: $headBufferLength] [ $splitLength ] [$tailBufferLength]")

        // Set up the body stream whose read method returns
        // -1 upon reaching the split boundry

        InputStream splitBoundedBodyStream =
                Channels.newInputStream(
                        new ReadableByteChannelWithConditionalBound<ReadableByteChannel>(
                                new ReadableByteChannelFromInputStream(stream),
                                xstream -> didHitSplitBound(stream, adjustedSplitEnd)));


        // Find the second record in the next split - i.e. after splitEnd (inclusive)
        // This is to detect record parts that although cleanly separated by the split boundary still need to be aggregated,
        // such as <g> { } | <g> { }   (where '|' denotes the split boundary)

        // If we are at start 0, we parse from the beginning - otherwise we skip the first record

        // println("HEAD BUFFER: " + new String(headBuffer, headBytes, headBufferLength - headBytes, StandardCharsets.UTF_8))
        // println("TAIL BUFFER: " + new String(tailBuffer, 0, tailBytes, StandardCharsets.UTF_8))

        // Assemble the overall stream
        InputStream preambleStream = new ByteArrayInputStream(preambleBytes);
        InputStream headStream = null;
        InputStream tailStream = null;
        InputStream postambleStream = null;

        if (headBytes < 0) {
            // FIXME There are two possibilities now why we couldn't find a record
            //  - There were errors in the data
            //  - There is a record that goes across this split
            logger.warn(String.format("No data found in split starting at %d. Possible reasons: " +
                    "(1) A large record spanning this split, " +
                    "(2) Syntax error(s) in the data," +
                    "(3) Some bug in the implementation", splitStart));

            // No data from this split
            headStream = new ByteArrayInputStream(EMPTY_BYTE_ARRAY);
            splitBoundedBodyStream = new ByteArrayInputStream(EMPTY_BYTE_ARRAY);
            tailStream = new ByteArrayInputStream(EMPTY_BYTE_ARRAY);
            postambleStream = new ByteArrayInputStream(EMPTY_BYTE_ARRAY);
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

            if (!isFirstSplit) {
                headStream = effectiveInputStream(headStream);
            }

            // Sanity check for non-encoded data: The body must immediately follow
            // the (adjusted) split start + the know header size
            if (!isEncoded) {
                long expectedBodyOffset = adjustedSplitStart + headBuffer.getKnownDataSize();
                // println(s"adjustedSplitStart=$adjustedSplitStart + known head buffer size = ${headBuffer.getKnownDataSize} = $expectedBodyOffset - actual body offset = ${stream.getPos}")

                if (expectedBodyOffset != stream.getPos()) {
                    throw new RuntimeException("Expected body offset does not match actual one: adjustedSplitStart = " + adjustedSplitStart + " known head buffer size = " + headBuffer.getKnownDataSize() + ", expected body offset = " + expectedBodyOffset + ", actual body offset = " + stream.getPos());
                }
            }

            // Displacement was needed due to hadoop reading one byte past split boundaries
            // With DeferredSeekablePushbackInputStream this should no longer be needed
            // // TailBuffer in encoded setting is displaced by 1 byte
            int displacement = isEncoded ? 1 : 0;

            Seekable tailChannel = tailBuffer.newChannel();
            tailChannel.nextPos(displacement);

            tailStream = new BoundedInputStream(Channels.newInputStream(tailChannel), tailBytes - displacement);

            // If there is a tailRecord then inject the postamble
            postambleStream = tailRecordOffset < 0
                    ? new ByteArrayInputStream(EMPTY_BYTE_ARRAY)
                    : new ByteArrayInputStream(postambleBytes);
        }

        boolean writeOutSegments = false;

        if (writeOutSegments) {
            // String splitName = split.getPath().getName();

            Path basePath = SystemUtils.getJavaIoTmpDir().toPath().toAbsolutePath();

            logger.info("Writing segment " + splitName + " " + splitStart + " to " + basePath);

            // Path basePath = Paths.get("/mnt/LinuxData/tailRecordOffset/");

            Path preambleFile = basePath.resolve(splitName + "_" + splitStart + ".preamble.dat");
            Path headFile = basePath.resolve(splitName + "_" + splitStart + ".head.dat");
            Path bodyFile = basePath.resolve(splitName + "_" + splitStart + ".body.dat");
            Path tailFile = basePath.resolve(splitName + "_" + splitStart + ".tail.dat");
            Path postambleFile = basePath.resolve(splitName + "_" + splitStart + ".postamble.dat");
            Files.copy(preambleStream, preambleFile);
            Files.copy(headStream, headFile);
            Files.copy(splitBoundedBodyStream, bodyFile);
            Files.copy(tailStream, tailFile);
            Files.copy(postambleStream, postambleFile);
            // Nicely close streams? Then again, must parts are in-memory buffers and this is debugging code only
            preambleStream = Files.newInputStream(preambleFile, StandardOpenOption.READ);
            headStream = Files.newInputStream(headFile, StandardOpenOption.READ);
            splitBoundedBodyStream = Files.newInputStream(bodyFile, StandardOpenOption.READ);
            tailStream = Files.newInputStream(tailFile, StandardOpenOption.READ);
            postambleStream = Files.newInputStream(postambleFile, StandardOpenOption.READ);
        }


        InputStream fullStream = InputStreamWithCloseLogging.wrap(new SequenceInputStream(Collections.enumeration(
                Arrays.asList(preambleStream, headStream, splitBoundedBodyStream, tailStream, postambleStream))),
                ExceptionUtils::getStackTrace, RecordReaderGenericBase::logClose);


        Stream<T> result = null;
        if (headBytes >= 0) {
            result = aggregate(isFirstSplit, parse(fullStream), tailItems);

            // val parseLength = effectiveRecordRangeEnd - effectiveRecordRangeStart
            // nav.setPos(effectiveRecordRangeStart - splitStart)
            // nav.limitNext(parseLength)
            // result = parser(nav)
            // .onErrorReturnItem(EMPTY_DATASET)
            // .filter(isNonEmptyDataset)
        } else {
            result = Stream.empty();
        }

        //    val cnt = result
        //      .count()
        //      .blockingGet()
        //    System.err.println("For effective region " + effectiveRecordRangeStart + " - " + effectiveRecordRangeEnd + " got " + cnt + " datasets")

        return result;
    }


    public static Stream<String> lines(Seekable seekable) {
        BufferedReader br = new BufferedReader(new InputStreamReader(Channels.newInputStream(seekable.cloneObject())));
        return br.lines().onClose(() -> IOUtils.closeQuietly(br));
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
            CustomPattern recordSearchPattern,
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

        CustomMatcher fwdMatcher = recordSearchPattern.matcher(charSequence);
        fwdMatcher.region(0, relProbeRegionEnd);

        long matchPos = findFirstPositionWithProbeSuccess(seekable, posValidator, fwdMatcher, true, prober);

        long result = matchPos >= 0
                ? matchPos + splitStart
                : -1;
        return result;
    }

    /**
     * Find the start of the nth record as seen from 'splitStart' (inclusive)
     * Only returns a result different from -1 if the nth record is found.
     *
     * @param n
     * @param nav
     * @param splitStart
     * @param absProbeRegionStart
     * @param maxRecordLength
     * @param absDataRegionEnd
     * @param prober
     * @return
     */
    long skipToNthRecord(
            int n,
            Seekable nav,
            long splitStart,
            long absProbeRegionStart,
            long maxRecordLength,
            long absDataRegionEnd,
            Predicate<Long> posValidator,
            Predicate<Seekable> prober) throws IOException {
        long result = -1L;

        // TODO availableDataRegion is a confusing name - what is meant is "the available amount *allowed* for probing"
        //   the *actual* available amount of data may be much less
        long availableDataRegion = absDataRegionEnd - absProbeRegionStart;
        long nextProbePos = absProbeRegionStart;
        for (int i = 0; i < n; ++i) {
            long candidatePos = findNextRecord(recordStartPattern, nav, splitStart, nextProbePos, maxRecordLength, absDataRegionEnd, posValidator, prober);
            if (candidatePos < 0) {
                // If there is more than maxRecordLength data available then
                // it is inconsistent for findNextRecord to indicate that no record was found
                // Either the maxRecordLength parameter is too small,
                // or there is an internal error with the prober
                // or there is a problem with the data (e.g. syntax error)
                if (availableDataRegion >= maxRecordLength) {
                    // TODO Improve the condition for showing this warning:
                    //   It only takes the data region *allowed* for probing into account instead of the *available* one.
                    //   For encoded streams the available region is not known in advance
                    //   Hence, the warning is e.g. always incorrectly shown when scanning the tail region of the last split.
                    logger.warn("Found no record start in a record search region of " + maxRecordLength + " bytes, although up to " + availableDataRegion + " bytes were allowed for reading");
                    // throw new RuntimeException(s"Found no record start in a record search region of $maxRecordLength bytes, although $availableDataRegion bytes were available")
                } else {
                    // Here we assume we read into the last chunk which contained no full record
                    logger.warn("No more records found after pos " + (splitStart + nextProbePos));
                }


                // Retain best found candidate position
                // effectiveRecordRangeEnd = dataRegionEnd
                break;
            } else {
                // If this is the last iteration set the result
                if (i + 1 == n) {
                    result = candidatePos;
                } else {
                    // If not in the last iteration then update the probe position
                    nextProbePos = candidatePos + minRecordLength;
                }
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
            CustomMatcher m,
            boolean isFwd,
            Predicate<Seekable> prober) throws IOException {

        Seekable seekable = rawSeekable.cloneObject();
        long absMatcherStartPos = seekable.getPos();

        boolean isEndReached = false;

        long result = -1l;

        StopWatch sw = StopWatch.createStarted();
        while (m.find() && !isEndReached) {
            // logger.info("Time spent on regex matching: " + sw.getTime(TimeUnit.MILLISECONDS) + "ms");

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

            // Set the seekable to the found position ...
            seekable.setPos(absPos);
            // .. which may reveal that we have actually reached the end
            isEndReached = seekable.isPosAfterEnd();

            if (!isEndReached) {
                // System.out.println("End reached: " + isEndReached);

                Seekable probeSeek = seekable.cloneObject();

                boolean probeResult = prober.test(probeSeek);
                // System.err.println(s"Probe result for matching at pos $absPos with fwd=$isFwd: $probeResult")

                if (probeResult) {
                    result = absPos;
                    break;
                }
            }
            sw.reset();
            sw.start();
        }

        return result;
    }


    @Override
    public boolean nextKeyValue() throws IOException {
        if (datasetFlow == null) {
            initRecordFlow();
        }

        boolean result;
        if (!datasetFlow.hasNext()) {

            if (raisedThrowable != null) {
                throw new RuntimeException(raisedThrowable);
            }

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
    public T getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException {
        // Very raw estimate; does not take head / tail buffers into accoun
        // Also, in the beginning the position is beyond th split end in order to find the end position
        // At this time the progress would be reported as 100%
        float result = this.splitStart == this.splitEnd
                ? 0.0F
                : Math.min(1.0F, (this.stream.getPos() - this.splitStart) / (float) (this.splitEnd - this.splitStart));
        return result;
    }

    @Override
    public void close() throws IOException {
        try {
            if (recordFlowCloseable != null) {
                recordFlowCloseable.run();
            }
        } finally {

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
}


// TODO Clean up below

/**
 * Flowable transform that skips the first n groups of a flowable
 *
 * @param skipGroupCount
 * @param classifier
 * @param <I>
 * @param <O>
 * @param <K>
 * @return
 */
/*
    public static <I, O, K> FlowableTransformer<I, O> skipGroups(long skipGroupCount, Function<? super I, ? extends K> classifier) {
        return upstream -> {
            long[] numGroupsSkipped = {0};
            boolean[] firstItemSeen = {false};
            Object[] lastSeenKey = {null};

            upstream.filter(item -> {
                boolean r;
                if (numGroupsSkipped[0] >= skipGroupCount) {
                    r = true;
                } else {
                    Object key = classifier.apply(item);
                    if (!firstItemSeen[0]) {
                        lastSeenKey[0] = key;
                        firstItemSeen[0] = true;
                    } else {
                        boolean isEqual = Objects.equals(lastSeenKey, key);
                        if (!isEqual) {
                            ++numGroupsSkipped[0];
                            lastSeenKey[0] = key;
                        }
                    }

                    r = numGroupsSkipped[0] >= skipGroupCount;
                }
                return r;
            });
        };
    }

    public static <I, O, K> FlowableTransformer<I, O> takeGroups(long takeGroupCount, Function<? super I, ? extends K> classifier) {
        return upstream -> {
            long[] numGroupsTaken = {0};
            boolean[] firstItemSeen = {false};
            Object[] lastSeenKey = {null};

            upstream.filter(item -> {
                boolean r;
                if (numGroupsTaken[0] >= takeGroupCount) {
                    r = false;
                } else {
                    Object key = classifier.apply(item);
                    if (!firstItemSeen[0]) {
                        lastSeenKey[0] = key;
                        firstItemSeen[0] = true;
                    } else {
                        boolean isEqual = Objects.equals(lastSeenKey, key);
                        if (!isEqual) {
                            ++numGroupsTaken[0];
                            lastSeenKey[0] = key;
                        }
                    }

                    r = numGroupsSkipped[0] >= skipGroupCount;
                }
                return r;
            });
        };
    }
*/

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
        /*
        Function<Seekable, Flowable<Dataset>> parser = seekable -> {
            Callable<InputStream> inSupp = () -> effectiveInputStreamSupp.apply(seekable);

            Flowable<Dataset> r = RDFDataMgrRx.createFlowableDatasets(inSupp, Lang.TRIG, null);
            return r;
        };
        */

// Predicate<T> isNonEmptyDataset = t -> !isEmptyRecord(t);
//        InputStream splitBoundedHeadStream =
//                Channels.newInputStream(
//                        new ReadableByteChannelWithConditionalBound<ReadableByteChannel>(
//                                new ReadableByteChannelWithoutCloseOnInterrupt(stream),
//                                xstream -> hitSplitBound.test(new SeekableInputStream(rawStream, rawStream), adjustedSplitEnd)));

// var splitBoundedBodyStream: InputStream = InputStreamWithCloseLogging.wrap(
// new CloseShieldInputStream(Channels.newInputStream(new ReadableByteChannelWithConditionalBound[ReadableByteChannel](Channels.newChannel(stream),
// xstream => hitSplitBound(stream, adjustedSplitEnd)))), ExceptionUtils.getStackTrace(_), logger.info(_))


// val rawPos = rawStream.getPos
// println(s"Adjusted: [$start, $end[ -> [$adjustedStart, $adjustedEnd[ - raw pos: $rawPos" )

// This wrapping creates a seekable input stream that advertises
// new positions after reading the byte before a block boundary
/*
                DeferredSeekablePushbackInputStream dspis =
                        new DeferredSeekablePushbackInputStream(
                                InputStreamWithCloseLogging.wrap(
                                        new InputStreamWithZeroOffsetRead(tmp),
                                        ExceptionUtils::getStackTrace, RecordReaderGenericBase::logUnexpectedClose),
                                tmp);


                // System.out.println("Requested pos: " + start + " Adjusted pos: " + adjustedStart + " Stream pos: " + tmp.getPos());


                stream = new SeekableInputStream(
                        new InputStreamWithCloseIgnore(dspis), dspis);
*/
                /*
                SeekablePushbackInputStream core = new SeekablePushbackInputStream(
                        new InputStreamWithCloseIgnore(InputStreamWithCloseLogging.wrap(
                                new InputStreamWithZeroOffsetRead(tmp),
                                ExceptionUtils::getStackTrace, RecordReaderGenericBase::logUnexpectedClose)),
                        tmp, 1);

                // TODO Maybe we need to call read before the position gets valid?
                // Read one byte from the stream to update the position
                // and push that byte back
                int b = tmp.read();
                if (b >= 0) {
                    core.unread(b);
                }
                adjustedStart = core.getPos();
                stream = new SeekableInputStream(core);
*/
