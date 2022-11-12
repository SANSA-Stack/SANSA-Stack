package net.sansa_stack.hadoop.core;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aksw.commons.io.buffer.array.ArrayOps;
import org.aksw.commons.io.buffer.array.BufferOverReadableChannel;
import org.aksw.commons.io.hadoop.SeekableInputStream;
import org.aksw.commons.io.input.CharSequenceDecorator;
import org.aksw.commons.io.input.ReadableChannel;
import org.aksw.commons.io.input.ReadableChannelOverIterator;
import org.aksw.commons.io.input.ReadableChannelSwitchable;
import org.aksw.commons.io.input.ReadableChannelWithValue;
import org.aksw.commons.io.input.ReadableChannels;
import org.aksw.commons.io.input.SeekableReadableChannel;
import org.aksw.commons.io.seekable.api.Seekable;
import org.aksw.commons.util.stack_trace.StackTraceUtils;
import org.aksw.commons.util.stream.SequentialGroupBySpec;
import org.aksw.commons.util.stream.StreamOperatorSequentialGroupBy;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.jena.ext.com.google.common.primitives.Ints;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sys.JenaSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.core.pattern.CustomMatcher;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.format.jena.base.RecordReaderConf;
import net.sansa_stack.hadoop.util.InputStreamWithCloseLogging;
import net.sansa_stack.hadoop.util.SeekableByteChannelFromSeekableInputStream;
import net.sansa_stack.io.util.InputStreamWithCloseIgnore;
import net.sansa_stack.io.util.InputStreamWithZeroOffsetRead;
import net.sansa_stack.nio.util.InterruptingSeekableByteChannel;

/**
 * A generic record reader that uses a callback mechanism to detect a consecutive sequence of records
 * that must start in the current split and which may extend over any number of successor splits.
 * The callback that needs to be implemented is {@link #parse(InputStream, boolean)}.
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
    static { JenaSystem.init(); }

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
    protected long maxRecordLength;
    protected long minRecordLength;
    protected int probeEltCount;

    protected boolean enableStats = true;


    protected final Accumulating<U, G, A, T> accumulating;



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

    protected String splitName;
    protected String splitId;

    protected long totalEltCount = 0;
    protected long totalRecordCount = 0;

    public RecordReaderGenericBase(RecordReaderConf conf, Accumulating<U, G, A, T> accumulating) {
        this.minRecordLengthKey = conf.getMinRecordLengthKey();
        this.maxRecordLengthKey = conf.getMaxRecordLengthKey();
        this.probeRecordCountKey = conf.getProbeRecordCountKey();
        this.recordStartPattern = conf.getRecordSearchPattern();

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
        probeEltCount = job.getInt(probeRecordCountKey, 100);

        split = (FileSplit) inputSplit;

        // By default use the given stream
        // We may need to wrap it with a decoder below
        // var stream: InputStream with fs.Seekable = split.getPath.getFileSystem(context.getConfiguration).open(split.getPath)
        org.apache.hadoop.fs.Path path = split.getPath();
        rawStream = path.getFileSystem(context.getConfiguration()).open(path);

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

        splitName = split.getPath().getName();
        splitId = splitName + ":" + splitStart + "+" + splitLength;

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
     * @param isProbe     Whether the parser should be configured for probing. For example, it is desireable to suppress porse errors
     *                    during probing. Also, for probing the parser may optimize itself for minimizing latency of yielding items
     *                    rather than overall throughput.
     * @return
     */
    protected abstract Stream<U> parse(InputStream inputStream, boolean isProbe);

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
            int maxExpectedExcess = 0; // isEncoded ? 1 : 0;

            if (exceededSplitPos > maxExpectedExcess) {
                logger.warn(String.format("Split %s: Exceeded split pos by %d bytes", splitId, exceededSplitPos));
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
     * @param tailElts The first set of group items after in the next split
     * @return
     */
    protected Stream<T> aggregate(boolean isFirstSplit, Stream<U> splitFlow, Stream<U> tailElts) {
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
                .transform(Stream.concat(splitFlow, tailElts))
                .map(e -> e.getValue() == null ? null : accumulating.accumulatedValue(e.getValue()));
        if (!isFirstSplit) {
            result = result.skip(1);
        }
        // Stream<T> result = entryStream.map(e -> accumulating.accumulatedValue(e.getValue()));
        return result;
    }

    /** This method is meant for overriding the input stream */
    protected InputStream effectiveInputStream(InputStream base) {
        return base;
    }

    protected InputStream effectiveInputStreamSupp(ReadableChannel<byte[]> seekable) {
        ReadableChannel<byte[]> newChannel = seekable; // seekable.cloneObject();
        InputStream r = new SequenceInputStream(
                new ByteArrayInputStream(preambleBytes),
                effectiveInputStream(ReadableChannels.newInputStream(newChannel)));
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

    protected Stream<U> parseFromSeekable(ReadableChannel<byte[]> seekable, boolean isProbe) {
        InputStream in = effectiveInputStreamSupp(seekable);
        Stream<U> r = parse(in, isProbe);
        return r;
    }

    /**
     *
     * @param seekable
     * @param resultBuffer
     * @return
     */
    protected boolean prober(SeekableReadableChannel<byte[]> seekable, BufferOverReadableChannel<U[]> resultBuffer) {
        long recordCount;

        ArrayOps<U[]> arrayOps = (ArrayOps)ArrayOps.OBJECT;
        Stream<U> stream = null;
        ReadableChannel<U[]> channel = null;
        ReadableChannel<byte[]> byteChannel = new ReadableChannelSwitchable<>(seekable.cloneObject());
        try {

            // System.out.println("here");
            // Runtime.getRuntime().gc();
            stream = parseFromSeekable(byteChannel, true);

            if (resultBuffer != null) {
                // Set up a readable channel with a 'payload value' that references the byte stream
                channel = ReadableChannels.withValue(ReadableChannels.wrap(stream, arrayOps), byteChannel);
                resultBuffer.truncate();
                resultBuffer.setDataSupplier(channel);
                resultBuffer.loadFully(probeEltCount, true);
                recordCount = resultBuffer.getKnownDataSize();
            } else {
                recordCount = stream.limit(probeEltCount).count();
                stream.close();
            }
        } catch (Throwable e) {
            if (channel != null) {
                // Closing the channel (if there is one) implies closing the stream
                IOUtils.closeQuietly(channel);
            } else if (stream != null) {
                stream.close();
            }

            IOUtils.closeQuietly(byteChannel);

            if (e instanceof IOException) {
                // Parse errors can be silently swallowed, however technical exceptions such as IO error
                //  need to be passed on!
                throw new RuntimeException(e);
            } else {
                recordCount = -1;
            }
        }
        // .collect(Collectors.toCollection(() -> new ArrayList<>(probeRecordCount)));

        boolean foundValidRecordOffset = recordCount > 0;

        // System.out.println(String.format("Probing at pos %s: %b", pos, foundValidRecordOffset));
        return foundValidRecordOffset;
    }

    SeekableSourceOverSplit source;


    /* Head state */
    LongPredicate posValidator;
    LongPredicate readPosValidator;
    Function<Long, Long> posToSplitId;

    ProbeResult headBytes = null; //-1;
    // Set the stream to the start of the split and get the head buffer
    // Note that we will use the stream later in its state to read the body part
    long knownDecodedDataLength = isEncoded ? -1 : splitLength; // TODO Rename to decodedSplitLength

    /* Tail state */
    protected int skipRegionCount = 2;
    protected long maxExtraByteCount = Long.MAX_VALUE;
    protected ProbeResult tailRecordOffset = null;
    protected long tailBytes = -1;
    protected BufferOverReadableChannel<U[]> tailEltBuffer;
    protected BufferOverReadableChannel<byte[]> tailByteBuffer;
    protected List<U> tailElts = Collections.emptyList();
    protected long tailEltsTime;

    protected Boolean regionStartSearchReadOverSplitEnd = null;
    protected Boolean regionStartSearchReadOverRegionEnd = null;

    public static ProbeStats convert(Resource tgt, ProbeResult src) {
        ProbeStats result =
                tgt.as(ProbeStats.class)
                .setCandidatePos(src.candidatePos())
                .setProbeCount(src.probeCount())
                .setTotalDuration(src.totalDuration.toNanos() * 1e-9);
        return result;
    }

    public Stats2 getStats() {
        Model m = ModelFactory.createDefaultModel();
//        JenaPluginUtils.registerResourceClass(Stats2.class, ProbeStats.class);
        Stats2 result = m.createResource().as(Stats2.class);

        // Stats result = new Stats();

        NavigableMap<Long, Long> blockMap = source.getAbsPosToBlockOffset();
        Long firstBlock = blockMap == null
                ? null
                : Optional.ofNullable(blockMap.firstEntry()).map(Entry::getValue).orElse(-1l);

        result
            .setSplitStart(splitStart)
            .setSplitSize(splitLength)
            .setFirstBlock(firstBlock)
            .setRegionStartProbeResult(headBytes == null ? null : convert(m.createResource(), headBytes))
            .setRegionEndProbeResult(tailRecordOffset == null ? null : convert(m.createResource(), tailRecordOffset))
            .setRegionStartSearchReadOverSplitEnd(regionStartSearchReadOverSplitEnd)
            .setRegionStartSearchReadOverRegionEnd(regionStartSearchReadOverRegionEnd)
            // .setRegionStart(headBytes == null ? -1 : headBytes.candidatePos())
            .setTailElementCount(tailElts == null ? null : tailElts.size())
            // .setTailRecordCount(totalRecordCount);
            .setTotalElementCount(totalEltCount)
            .setTotalRecordCount(totalRecordCount)
            .setTotalBytesRead(source.getKnownSize())
            ;

        return result;
    }

//    protected void detectTail(InputStream in) {
//        tailByteBuffer = BufferOverReadableChannel.createForBytes(
//                new BoundedInputStream(in, maxExtraByteCount), 1024 * 1024);

    protected void detectTail(BufferOverReadableChannel<byte[]> tailByteBuffer) {
        BufferOverReadableChannel<U[]> tailEltBuffer = BufferOverReadableChannel.createForObjects(probeEltCount);

        long maxExtraBytes = 1_000_000;
        LongPredicate tailCharPosValidator = pos -> {
            boolean r = true;
            if (tailByteBuffer.isDataSupplierConsumed()) {
                long maxPos = tailByteBuffer.getKnownDataSize() + maxExtraBytes;
                r = pos < maxPos;
            }
            return r;
        };

        try (SeekableReadableChannel<byte[]> tailByteChannel = tailByteBuffer.newReadableChannel()) {
            StopWatch tailSw = StopWatch.createStarted();
            tailRecordOffset = skipToNthRegionInSplit(skipRegionCount, tailByteChannel, 0, 0, maxRecordLength, maxExtraByteCount, tailCharPosValidator, pos -> true, posToSplitId, tailEltBuffer, this::prober);

            // If no record is found in the tail then take all its known bytes because
            // we assume we hit the last few splits of the stream and there simply are no further record
            // starts anymore
            tailBytes = tailRecordOffset.candidatePos() < 0 ? tailByteBuffer.getKnownDataSize() : Ints.checkedCast(tailRecordOffset.candidatePos());

            logger.info(String.format("Split %s: Got %d tail bytes within %.3f s", splitId, tailBytes, tailSw.getTime(TimeUnit.MILLISECONDS) * 0.001));

            if (tailRecordOffset.candidatePos() >= 0) {
                // Now that we found an offset in the tail region, read out one more complete list of items that belongs to one group
                // Note, that these items may need to be aggregated with items from the current split - that's why we retain them as a list
                // tailNav.position(tailBytes);
                // lines(tailNav).limit(100).forEach(System.out::println);

                SequentialGroupBySpec<U, G, List<U>> spec = SequentialGroupBySpec.create(accumulating::classify, () -> (List<U>) new ArrayList<>(1 * 1024), Collection::add);

                try (Stream<U> tailEltStream = debufferedEltStream(tailEltBuffer)) {
                    tailElts = StreamOperatorSequentialGroupBy.create(spec).transform(tailEltStream).map(Map.Entry::getValue).findFirst().orElse(Collections.emptyList());
                }

                long adjustedSplitEnd = -1;
                tailEltsTime = tailSw.getTime(TimeUnit.MILLISECONDS);
                logger.info(String.format("Split %s: Got %d tail items at pos %d within %d bytes read in %d ms", splitId, tailElts.size(), adjustedSplitEnd, tailBytes, tailEltsTime));
                //}
            }

            ReadableChannel<U[]> tmp = tailEltBuffer.getDataSupplier();
            if (tmp != null) {
                tmp.close();
            }

            // We buffered the tail elements - close the byte supplier
            tailByteBuffer.getDataSupplier().close();
        } catch (Throwable e) {
            // The logic that analyses and possibly gathers tail elements should never fail
            // TODO Unless due to IOExceptions e.g. when the network went down
            throw new RuntimeException("Should never come here", e);
        }
    }

    protected Stream<T> createRecordFlow() throws IOException {
        logger.info("Processing split " + splitId);

        // Except for the first split, the first region may parse successfully but may
        // actually be an incomplete element that is cut off at the split boundary and that would
        // thus be interpreted incorrectly. For example, a quad followed by a triple such as
        // "<s> | <p> <o> <g> . <s> <p> <o> ." with "|" indicating the
        // split boundary would be incorrectly seen as a triple in the default graph.
        // The second element may be wrongly connected to the incomplete record, so the third region
        // should be safe. Once we find the start of the third region we
        // then need to find probeRecordCount further elements to validate the starting position.

        // int skipRegionCount = 2;
        // long maxExtraByteCount = (skipRegionCount + probeEltCount) * maxRecordLength;

        // Set the stream to the end of the split and get the tail buffer
        Map.Entry<Long, Long> adjustedTailSplitBounds = setStreamToInterval(splitStart, Long.MAX_VALUE); //splitEnd + maxExtraByteCount);
        //long adjustedSplitEnd = adjustedTailSplitBounds.getKey();

        // TODO Somehow make the stream capable of limiting itself on read across the split boundary
        //  if a head region has been detected.

        // SeekableReadableChannel<byte[]> sstream = SeekableInputStreams.wrap(stream);
         source = isEncoded
                ? SeekableSourceOverSplit.createForBlockEncodedStream(stream, splitEnd, postambleBytes)
                : SeekableSourceOverSplit.createForNonEncodedStream(stream, splitEnd, postambleBytes);

        SeekableSourceOverSplit.Channel headByteChannel = source.newReadableChannel();
        // System.out.println(splitId + ": Opened " + ObjectUtils.identityToString(headByteChannel));
        // Pos validator: Returns true as long as the position is not past the split bound w.r.t. decoded data
        posValidator = pos -> {
            boolean r = !source.getHeadBuffer().isDataSupplierConsumed() || pos < source.getHeadBuffer().getKnownDataSize();
            // logger.info("Validation request: " + pos + " known size=" + knownDecodedDataLength[0] + " valid=" + r);
            return r;
        };


        long maxExtraBytes = 1_000_000;
        readPosValidator = pos -> {
            boolean r = true;
            boolean inHead = posValidator.test(pos);
            if (!inHead) {
                long size = source.getHeadBuffer().getKnownDataSize();
                long maxPos = size + maxExtraBytes; // Allow that many more bytes for read during pattern matching
                // TODO Make configurable
                r = pos < maxPos;
            }
            return r;
        };

        Function<Long, Long> idfn = TailBufferChannel.splitIdFn(splitStart, splitLength);
        posToSplitId = !isEncoded ? idfn :
            pos -> {
                long blockOffset = source.getBlockForPos(pos);
                long sid = idfn.apply(blockOffset);
                return sid;
            };

        BufferOverReadableChannel<U[]> headEltBuffer = BufferOverReadableChannel.createForObjects(probeEltCount);

        StopWatch headSw = StopWatch.createStarted();

        headBytes = isFirstSplit
                ? new ProbeResult(0, 0, Duration.ZERO)
                : skipToNthRegionInSplit(skipRegionCount, headByteChannel, 0, 0, maxRecordLength, maxExtraByteCount, readPosValidator, posValidator, posToSplitId, headEltBuffer, this::prober);

        long headRecordTime = headSw.getTime(TimeUnit.MILLISECONDS);
        logger.info(String.format("Split %s: Found head region offset at pos %d in %.3f s",
                splitId,
                headBytes.candidatePos(),
                // -1, // source.getKnownDataSize(),
                headSw.getTime(TimeUnit.MILLISECONDS) * 0.001f));

        if (false) {
            try (SeekableReadableChannel<byte[]> channel = source.newReadableChannel(headBytes.candidatePos())) {
                System.out.println("GOT: " + IOUtils.toString(ReadableChannels.newInputStream(channel), StandardCharsets.UTF_8));
            }
            return Stream.empty();
        }

        ReadableChannel<U[]> headEltChannel = null;
        Stream<U> headItems = null;

        if (headBytes.candidatePos() < 0) {
            SeekableSourceOverSplit.Channel finalHeadByteChannel2 = headByteChannel;
            return Stream.<T>empty().onClose(() -> {
                try {
                    finalHeadByteChannel2.close();
                    source.close();
                    // this.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }


        if (!isFirstSplit) {
            // One ugly cast here in order to avoid distributing all the long names and generics in the code
            // Extract the byte channel on which the parser currently runs
            ReadableChannelWithValue<U[], ReadableChannelSwitchable<byte[]>, ?> typedHeadEltChannel = (ReadableChannelWithValue<U[], ReadableChannelSwitchable<byte[]>, ?>) headEltBuffer.getDataSupplier();
            headByteChannel.close();
            headByteChannel = (SeekableSourceOverSplit.Channel)typedHeadEltChannel.getValue().getDecoratee();
            headEltChannel = typedHeadEltChannel;
            // headEltChannel = headEltBuffer.newReadableChannel()
            // BufferOverReadableChannel.debuffer(typedHeadEltChannel); // typedHeadEltChannel;
        } else {
            headItems = parseFromSeekable(headByteChannel, false);
        }



        // Block reads from the base stream in order to decide the next course of action.
        Lock lock = headByteChannel.getReadWriteLock().writeLock();
        lock.lock();

        try {
            if (headByteChannel.isHeadStream()) {
                // System.out.println("CASE1");
                regionStartSearchReadOverSplitEnd = false;
                regionStartSearchReadOverRegionEnd = false;

                headByteChannel.debufferHead();

                // (a) If the tail buffer has not yet been touched then schedule tail search as the stream's transition action
                SeekableSourceOverSplit.Channel finalHeadByteChannel = headByteChannel;
                headByteChannel.setTransitionAction(() -> {
                    logger.info("Transitioned to tail on byte: " + finalHeadByteChannel.getEnclosingInstance().getHeadBuffer().getKnownDataSize());

                    // System.err.println("Pos before: " + finalHeadByteChannel.position());
                    detectTail(source.getTailBuffer());
                    // System.err.println("Pos after: " + finalHeadByteChannel.position());

                    if (tailBytes >= 0) {
                        long absTailPos = source.getHeadSize() + tailBytes;
                        // long absTailPos = source.getHeadBuffer().getKnownDataSize() + tailBytes;
                        finalHeadByteChannel.setLimit(absTailPos);
                    }
                });
            } else {
                // System.out.println("CASE2");
                regionStartSearchReadOverSplitEnd = true;
                if (true) {
                    // throw new RuntimeException("Read over tail");
                }
                // (b) Otherwise start the tail search immediately. Once the tail region offset is
                //   found there are 2 possibilities:
                detectTail(source.getTailBuffer());
                long pos = headByteChannel.position();
                long absTailPos = source.getHeadBuffer().getKnownDataSize() + tailBytes;
                regionStartSearchReadOverRegionEnd = pos > absTailPos;

                // if (pos <= absTailPos) {
                if (!regionStartSearchReadOverRegionEnd) {
                    // System.out.println("CASE2A");

                    //   (b1) The tail region offset is greater than the current byte read offset - in that case just limit the byte stream
                    if (tailBytes >= 0) {
                        headByteChannel.setLimit(absTailPos);
                    }
                } else {
                    // System.out.println("CASE2B");
                    //   (b2) We already read past the end; restart the parser.
                    // Close the (possibly asynchronously running) parser
                    // Danger of deadlock: We have to release the lock because the close method wants acquire the lock too!
                    //
                    lock.unlock();
                    if (headEltBuffer.getDataSupplier() != null) {
                        headEltBuffer.getDataSupplier().close();
                    }
                    lock.lock();
                    headEltBuffer.setDataSupplier(null);
                    headEltBuffer.truncate();
                    headByteChannel.close();
                    headByteChannel = source.newReadableChannel();
                    // lock = headByteChannel.getReadWriteLock().writeLock();
                    // lock.lock();
                    headByteChannel.position(headBytes.candidatePos());
                    if (tailBytes >= 0) {
                        headByteChannel.setLimit(absTailPos);
                    }
                    headItems = parseFromSeekable(headByteChannel, false);
//                        headEltBuffer.getDataSupplier().close();
//                        headEltBuffer.truncate();
//                        headEltBuffer.setDataSupplier(headByteChannel);
//
//                        headEltChannel = headEltBuffer
                }
            }
        } finally {
            lock.unlock();
        }

        if (false) {
            headByteChannel.position(headBytes.candidatePos());
            //try (SeekableReadableChannel<byte[]> channel = ){
                System.out.println("GOT: " + IOUtils.toString(ReadableChannels.newInputStream(headByteChannel), StandardCharsets.UTF_8));
            //}
            return Stream.empty();
        }

        if (headItems == null) {
            // headEltChannel = headEltBuffer.getDataSupplier();
            headItems = ReadableChannels.newStream(headEltChannel);
            headItems = Stream.concat(ReadableChannels.newStream(headEltBuffer.getBuffer().newReadableChannel()), headItems);
            // headItems = ReadableChannels.newStream(headEltBuffer.newReadableChannel());
        }

        ReadableChannel<U[]> finalHeadEltChannel = headEltChannel;
        if (finalHeadEltChannel != null) {
            headItems = headItems
                    .onClose(() -> {
                        try {
                            finalHeadEltChannel.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }


        // This stream has to be lazy because the tailElts attribute is only
        // initialized when the split boundary is hit
        Stream<U> lazyTailElts = Stream.of(1).flatMap(x -> {
            logger.info("Yielding " + tailElts.size() + " tail elements");
            return tailElts.stream();
        });

        if (enableStats) {
            headItems = headItems.peek(x -> ++totalEltCount);
        }

        SeekableSourceOverSplit.Channel finalHeadByteChannel1 = headByteChannel;
        Stream<T> result = aggregate(isFirstSplit, headItems, lazyTailElts)
                .onClose(() -> {
                	// System.out.println(splitId + " Closed: " + ObjectUtils.identityToString(finalHeadByteChannel1));
                    finalHeadByteChannel1.close();
                    try {
                        source.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        if (false) {
            List<T> tmp = result.collect(Collectors.toList());
            System.err.println(String.format("Split %s: Got %d items in total", splitId, tmp.size()));
            result = tmp.stream();
        }

        if (enableStats) {
            result = result.peek(x -> ++totalRecordCount);
        }

/*
        System.out.println("Is first split? " + isFirstSplit);
        headItems.forEach(x -> System.out.println("Head Item: " + x));
        lazyTailElts.forEach(x -> System.out.println("Tail Item: " + x));

        Stream<T> result = Stream.empty();
*/
        return result;
    }


    public static Stream<String> lines(Seekable seekable) {
        BufferedReader br = new BufferedReader(new InputStreamReader(Channels.newInputStream(seekable.cloneObject())));
        return br.lines().onClose(() -> IOUtils.closeQuietly(br));
    }

    public static <T> Stream<T> unbufferedStream(BufferOverReadableChannel<T[]> borc) {
        try {
            Stream<T> buffered = ReadableChannels.newStream(borc.getBuffer().newReadableChannel());
            ReadableChannelWithValue<T[], ?, ReadableChannelOverIterator<T>> eltSource = (ReadableChannelWithValue<T[], ?, ReadableChannelOverIterator<T>>) borc.getDataSupplier();

            Stream<T> unbuffered = eltSource.getDecoratee().toStream();
            return Stream.concat(buffered, unbuffered);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static <T> Stream<T> debufferedEltStream(BufferOverReadableChannel<T[]> eltBorc) {
        ReadableChannel<T[]> dataSupplier = eltBorc.getDataSupplier();

        boolean suppressDebuffer = false;
        if (suppressDebuffer) {
            try {
                return ReadableChannels.newStream(eltBorc.newReadableChannel()).onClose(() -> IOUtils.closeQuietly(eltBorc.getDataSupplier()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        ReadableChannelWithValue<T[], ReadableChannelSwitchable<byte[]>, ?> byteSource = (ReadableChannelWithValue<T[], ReadableChannelSwitchable<byte[]>, ?>)  eltBorc.getDataSupplier();

        BufferOverReadableChannel.debuffer(byteSource.getValue());

        return unbufferedStream(eltBorc).onClose(() -> IOUtils.closeQuietly(dataSupplier));
        // ReadableChannelSwitchable<byte[]> byteSwitchable = byteC.getValue();
        // debuffer(byteBorc, byteC.getValue());

        // ReadableChannelWithValue<U, ReadableChannelSwitchable<U>, ?> eltC = (ReadableChannelWithValue<U, ReadableChannelSwitchable<U>, ?>) eltChannel;
        // debuffer(eltBorc, eltC.getValue());
    }


    /** Remove buffering from a channel. As long as the channel is positioned within the buffer's store
     * area it will read from the buffer's store but no longer write to it.
     * After leaving that area, data is read directly from the buffer's supplier */
    /*
    public static <A> void debuffer(BufferOverReadableChannel<A> borc, ReadableChannel<A> channel) {
        ReadableChannelSwitchable<A> switchable = (ReadableChannelSwitchable<A>)channel;
        ReadableChannel<A> decoratee = switchable.getDecoratee();

        if (decoratee instanceof BufferOverReadableChannel.Channel) {
            @SuppressWarnings("unchecked")
            BufferOverReadableChannel.Channel<A> c = (BufferOverReadableChannel.Channel<A>)decoratee;

            Lock writeLock = switchable.getReadWriteLock().writeLock();
            try {
                writeLock.lock();
                Buffer<A> buffer = borc.getBuffer();
                long bufferSize = buffer.size();

                long pos = c.getReadCount();
                ReadableChannel<A> dataSupplier = borc.getDataSupplier();
                ReadableChannel<A> newChannel;
                if (pos < bufferSize) {
                    newChannel = ReadableChannels.concat(Arrays.asList(
                            buffer.newReadableChannel(pos), dataSupplier));
                } else {
                    newChannel = dataSupplier;
                }
                switchable.setDecoratee(newChannel);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                writeLock.unlock();
            }
        }
    }*/

    public static class ReadTooFarException
        extends RuntimeException {

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
    public static <U> ProbeResult findNextRegion(
            CustomPattern recordSearchPattern,
            SeekableReadableChannel<byte[]> nav,
            long splitStart,
            long absProbeRegionStart,
            long maxRecordLength,
            long absDataRegionEnd,
            LongPredicate matcherReadPosValidator, // Runtime validation
            LongPredicate posValidator, // Validate the position of a found match
            BufferOverReadableChannel<U[]> outBuffer,
            BiPredicate<SeekableReadableChannel<byte[]>, BufferOverReadableChannel<U[]>> prober) throws IOException {
        // Set up absolute positions
        long absProbeRegionEnd = Math.min(absProbeRegionStart + maxRecordLength, absDataRegionEnd); // = splitStart + bufferLength
        int relProbeRegionEnd = Ints.checkedCast(absProbeRegionEnd - absProbeRegionStart);

        long position = absProbeRegionStart - splitStart;
        nav.position(position);
        // System.err.println(s"absProbeRegionStart: $absProbeRegionStart - absProbeRegionEnd: $absProbeRegionEnd - relProbeRegionEnd: $relProbeRegionEnd")

        // Data region is up to the end of the buffer
        long relDataRegionEnd = absDataRegionEnd - absProbeRegionStart;

        try (SeekableReadableChannel<byte[]> seekable = nav.cloneObject()) { //ReadableChannels.shift(nav.cloneObject(), position)) {
            // seekable.position(0); // Position at the shifted position
            // seekable.position(absProbeRegionStart - splitStart);


            // seekable.limitNext(relDataRegionEnd)
            // val charSequence = new CharSequenceFromSeekable(seekable)
            // seekable.limitNext(relDataRegionEnd)
            // TODO The original code used limitNext but do we need that
            //  if we set the matcher region anyway?

            CharSequence charSequence = ReadableChannels.asCharSequence(seekable);
            charSequence = new CharSequenceDecorator(charSequence) {
                @Override
                public char charAt(int index) {
                    long readPosInSplit = position + index;
                    if (!matcherReadPosValidator.test(readPosInSplit)) {
                        throw new ReadTooFarException();
                    }

                    // Check whether the index is too far beyond the split point
                    return super.charAt(index);
                }
            };
            CustomMatcher fwdMatcher = recordSearchPattern.matcher(charSequence);
            fwdMatcher.region(0, relProbeRegionEnd);

            ProbeResult matchPosR = findFirstPositionWithProbeSuccess(seekable, posValidator, fwdMatcher, true, outBuffer, prober);
            long matchPos = matchPosR.candidatePos();

            long adjustedMatchPos = matchPos >= 0
                    ? matchPos + splitStart
                    : -1;

            return new ProbeResult(adjustedMatchPos, matchPosR.probeCount(), matchPosR.totalDuration());

            // return result;
        }
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
    ProbeResult skipToNthRegionInSplit(
            int n,
            SeekableReadableChannel<byte[]> nav,
            long splitStart,
            long absProbeRegionStart,
            long maxRecordLength,
            long absDataRegionEnd,
            LongPredicate matcherReadPosValidator, // Runtime validation
            LongPredicate posValidator,
            Function<Long, Long> posToSplitId,
            BufferOverReadableChannel<U[]> outBuffer,
            BiPredicate<SeekableReadableChannel<byte[]>, BufferOverReadableChannel<U[]>> prober) throws IOException {
        ProbeResult result = null; // -1L;

        long previousSplitId = -1;

        // TODO availableDataRegion is a confusing name - what is meant is "the available amount *allowed* for probing"
        //   the *actual* available amount of data may be much less
        long availableDataRegion = absDataRegionEnd - absProbeRegionStart;
        long nextProbePos = absProbeRegionStart;
        for (int i = 0; i < n; ++i) {
            boolean isLastIteration =  i + 1 == n;
            // Only buffer items on the last iteration
            BufferOverReadableChannel<U[]> effOutBuffer = isLastIteration ? outBuffer : null;

            result = findNextRegion(recordStartPattern, nav, splitStart, nextProbePos, maxRecordLength, absDataRegionEnd, matcherReadPosValidator, posValidator, effOutBuffer, prober);
            long candidatePos = result.candidatePos();
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
                    logger.warn(String.format("Split %s: Found no record start in a search region of " + maxRecordLength + " bytes, although up to " + availableDataRegion + " bytes were allowed for reading",
                            splitId));

                    // throw new RuntimeException(s"Found no record start in a record search region of $maxRecordLength bytes, although $availableDataRegion bytes were available")
                } else {
                    // Here we assume we read into the last chunk which contained no full record
                    logger.warn(String.format("Split %s: No more records found after pos " + (splitStart + nextProbePos),
                            splitId));
                }


                // Retain best found candidate position
                // effectiveRecordRangeEnd = dataRegionEnd
                break;
            } else {
                // If the current match is in a different split than the previous then start over counting
                // the matches in the split
                long splitId = posToSplitId.apply(candidatePos);
                if (splitId != previousSplitId) {
                    if (i > 0) {
                        isLastIteration = false;
                        // TODO Do we need to extract the byte level data supplier and close it too - or is this done implicitly?
                        outBuffer.getDataSupplier().close();
                        outBuffer.truncate();
                        i = 0;
                    }

                    previousSplitId = splitId;
                }

                // If this is the last iteration set the result
                if (isLastIteration) {
                    // result = candidatePosR;
                    // nothing to do
                } else {
                    // If not in the last iteration then update the probe position
                    nextProbePos = candidatePos + minRecordLength;
                }
            }
        }

        return result;
    }

    /**
     * Uses the matcher to find candidate probing positions, and returns the first position
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
    public static <U> ProbeResult findFirstPositionWithProbeSuccess(
            SeekableReadableChannel<byte[]> rawSeekable,
            LongPredicate posValidator,
            CustomMatcher m,
            boolean isFwd,
            BufferOverReadableChannel<U[]> outBuffer,
            BiPredicate<SeekableReadableChannel<byte[]>, BufferOverReadableChannel<U[]>> prober) throws IOException {

        long result = -1l;
        long probeCount = 0;

        StopWatch swTotal = StopWatch.createStarted();
        try (SeekableReadableChannel<byte[]> seekable = rawSeekable.cloneObject()) {
            long absMatcherStartPos = seekable.position();

            boolean showExcerpt = false;
            if (showExcerpt) {
                try (SeekableReadableChannel<byte[]> tmp = seekable.cloneObject()) {
                    System.out.println("Probing at pos " + absMatcherStartPos + ":\n" + abbreviateAsUTF8(ReadableChannels.newInputStream(tmp), 1024, "..."));
                }
            }

            boolean isEndReached = false;

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

                ++probeCount;
                boolean validAbsPos = posValidator.test((long) absPos);
                // System.out.println("ValidPos: " + validAbsPos);
                if (!validAbsPos) {
                    break;
                }

                // Set the seekable to the found position ...
                seekable.position(absPos);
                // .. which may reveal that we have actually reached the end
                isEndReached = seekable.read(new byte[1], 0, 1) <= 0; // == -1
                seekable.position(absPos);
                // isEndReached = seekable.isPosAfterEnd();

                if (!isEndReached) {

                    boolean probeResult;

                    if (showExcerpt) {
                        try (SeekableReadableChannel<byte[]> tmp = seekable.cloneObject()) {
                            System.out.println("Searching for candidate at pos " + tmp.position() + ":\n" + abbreviateAsUTF8(ReadableChannels.newInputStream(tmp), 1024, "..."));
                        }
                    }

                    try (SeekableReadableChannel<byte[]> probeSeek = seekable.cloneObject()) {
                        probeResult = prober.test(probeSeek, outBuffer);
                    }
                    // System.err.println(String.format("Probe result for matching at pos %d with fwd=%b %b", absPos, isFwd, probeResult));

                    if (probeResult) {
                        result = absPos;
                        break;
                    }
                } else {
                    // System.out.println("End reached: " + isEndReached);
                }
                sw.reset();
                sw.start();
            }
        } catch (ReadTooFarException e) {
            logger.info("Matcher raised 'read too far' exception which indicates that the read threshold for bytes past the split end has been reached");
            result = -1;
        }

        return new ProbeResult(result, probeCount, Duration.ofMillis(swTotal.getTime(TimeUnit.MILLISECONDS)));
    }


    @Override
    public boolean nextKeyValue() throws IOException {
        if (datasetFlow == null) {
            initRecordFlow();
        }

        boolean result;
        try {
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
        } catch (Exception e) {
            throw new RuntimeException(splitId, e);
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
        // TODO Add a config option to log summary stats
        boolean logSummaryStats = false;
        if (logSummaryStats) {
            RDFDataMgr.write(System.err, getStats().getModel(), RDFFormat.TURTLE_PRETTY);
        }

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


    public static String abbreviateAsUTF8(InputStream in , int maxWidth, String abbrevMarker) throws IOException {
        return abbreviate(in, StandardCharsets.UTF_8, maxWidth, abbrevMarker);
    }

    public static String abbreviate(InputStream in, Charset charset, int maxWidth, String abbrevMarker) throws IOException {
        return abbreviate(new InputStreamReader(in, charset), maxWidth, abbrevMarker);
    }

    public static String abbreviate(InputStreamReader reader, int maxWidth, String abbrevMarker) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        for (int i = 0; i < maxWidth && (c = reader.read()) != -1; ++i) {
            sb.append((char)c);
        }

        // Append the marker unless read successfully returns -1
        boolean appendAbbrevMarker = true;
        try {
            appendAbbrevMarker = reader.read() != -1;
        } catch (Exception e) {
            // Ignored
        }

        if (appendAbbrevMarker) {
            sb.append(abbrevMarker);
        }

        return sb.toString();
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

/*


        long adjustedSplitStart = -1;
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
/*
        InputStream splitBoundedBodyStream =
                Channels.newInputStream(
                        new ReadableByteChannelWithConditionalBound<ReadableByteChannel>(
                                new ReadableByteChannelFromInputStream(stream),
                                xstream -> didHitSplitBound(stream, adjustedSplitEnd)));
* /


// Find the second record in the next split - i.e. after splitEnd (inclusive)
// This is to detect record parts that although cleanly separated by the split boundary still need to be aggregated,
// such as <g> { } | <g> { }   (where '|' denotes the split boundary)

// If we are at start 0, we parse from the beginning - otherwise we skip the first record

// println("HEAD BUFFER: " + new String(headBuffer, headBytes, headBufferLength - headBytes, StandardCharsets.UTF_8))
// println("TAIL BUFFER: " + new String(tailBuffer, 0, tailBytes, StandardCharsets.UTF_8))

// Assemble the overall stream
InputStream preambleStream = new ByteArrayInputStream(preambleBytes);
    InputStream headStream = null;
// InputStream tailStream = null;
// InputStream postambleStream = null;

        if (headBytes < 0) {
        // FIXME There are two possibilities now why we couldn't find a record
        //  - There were errors in the data
        //  - There is a record that goes across this split
        logger.warn(String.format("Split %s: No data found. Possible reasons: " +
        "(1) A large record spanning this split, " +
        "(2) Syntax error(s) in the data," +
        "(3) Some bug in the implementation", splitId));

        // tailByteStream.close();

        // No data from this split
        headStream = new ByteArrayInputStream(EMPTY_BYTE_ARRAY);
        // splitBoundedBodyStream = new ByteArrayInputStream(EMPTY_BYTE_ARRAY);
        // tailByteStream = new ByteArrayInputStream(EMPTY_BYTE_ARRAY);
        // postambleStream = new ByteArrayInputStream(EMPTY_BYTE_ARRAY);
        } else if (false) {

        // headStream = new ByteArrayInputStream(headBuffer, headBytes, headBufferLength - headBytes)
        // We need to check the headLength here
            /*
            long headLength = -1;
            knownDecodedDataLength[0] < 0
                    ? headByteBuffer.getKnownDataSize()
                    : knownDecodedDataLength[0];

            SeekableReadableChannel<byte[]> headChannel = headByteBuffer.newReadableChannel();
            if (headBytes != 0) {
                // The method nextPos may read bytes from the head buffer if no bytes have been read yet!
                // So only call nextPos when we have to and know for sure we had prior reads. This is indicated by
                // a non-zero value of headBytes.
                // headChannel.nextPos(headBytes);
                headChannel.position(headBytes);
            }

            headStream = new BoundedInputStream(ReadableChannels.newInputStream(headChannel), headLength - headBytes);
*/
            /* This should no longer be needed
            if (!isFirstSplit) {
                headStream = effectiveInputStream(headStream);
            }
            * /
        } else if (tailRecordOffset < 0) {
        // We previously gave up on the search for a tail record offset within a limited range of data
        // But because we found a head record we now we need to find the tail record

        //


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
        // Files.copy(splitBoundedBodyStream, bodyFile);
        // Files.copy(tailByteStream, tailFile);
        // Files.copy(postambleStream, postambleFile);
        // Nicely close streams? Then again, must parts are in-memory buffers and this is debugging code only
        preambleStream = Files.newInputStream(preambleFile, StandardOpenOption.READ);
        headStream = Files.newInputStream(headFile, StandardOpenOption.READ);
        // splitBoundedBodyStream = Files.newInputStream(bodyFile, StandardOpenOption.READ);
        // tailByteStream = Files.newInputStream(tailFile, StandardOpenOption.READ);
        // postambleStream = Files.newInputStream(postambleFile, StandardOpenOption.READ);
        }




        Stream<T> result = null;
        if (headBytes >= 0) {

        // If probing the head records read past the tail record offset we have to reread the data in order to ensure
        // we use the right bounds
        // Otherwise, we can update the head buffers data supplier's limit to the tail offset


                /*
                InputStream fullStream = InputStreamWithCloseLogging.wrap(new SequenceInputStream(Collections.enumeration(
                                Arrays.asList(preambleStream, headStream, splitBoundedBodyStream, tailStream, postambleStream))),
                        ExceptionUtils::getStackTrace, RecordReaderGenericBase::logClose);
                 * /
        // ReadableChannel<byte[]> tailChannel = ReadableChannels.wrap(tailStream);

        Stream<U> bodyEltStream;
        if (headEltChannel == null) {
        bodyEltStream = parse(ReadableChannels.newInputStream(source.newReadableChannel()), false);
        }
        else {
        headByteChannel = null; // headEltChannel.getValue();
                /*
                if (headBytes >= 0) {
                    headByteChannel = headEltChannel.getValue();
                } else {
                    headByteChannel = ReadableChannels.withCounter(ReadableChannels.(parse(splitBoundedBodyStream, false)));
                }
                 * /

        // Switch the head channel to non-buffering.
        // This has to be done atomically because the parser may still be reading
        if (false) {
//                    headByteChannelSwitchable.setDecoratee(() -> {
//                        ReadableChannel<byte[]> r;

        // TODO Position check has to be synchronized because the parser may yet be reading ahead!
                    /*
                    long streamPos = 0;
                    try {
                        streamPos = stream.getPos();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    boolean isHeadPastTail = !posValidator.test(streamPos);

                    if (isHeadPastTail) {
                        throw new RuntimeException("Parsed beyord tail - not yet handled");
                    } else {
                     * /
//                        try {
//                            long n = headByteChannel.getReadCount();
//                            Buffer<byte[]> buffer = headByteBuffer.getBuffer();
//
//                            long size = buffer.size();
//
//                            ReadableChannel<byte[]> unbufferedHeadStream = ReadableChannels.wrap(bodyByteStream);
//
//                            List<ReadableChannel<byte[]>> channels = n <= size
//                                    ? Arrays.asList(buffer.newReadableChannel(n), unbufferedHeadStream)
//                                    : Arrays.asList(unbufferedHeadStream);
//
//
//                            // headByteChannelSwitchable.getDecoratee().close();
//
//                            r = ReadableChannels.concat(channels);
//                        } catch (Exception e) {
//                            throw new RuntimeException(e);
//                        }
//                        //}
//                        return r;
//                    });
//
//                    if (headByteChannelSwitchable.getDecoratee() == null) {
//                        throw new RuntimeException("Parsed beyond tail - not handled yet");
//                    }
        }
                /*
                Stream<U> headItems = ReadableChannels.newStream(headEltBuffer.getBuffer().newReadableChannel());
                Stream<U> bodyItems = ReadableChannels.newStream(headEltBuffer.getDataSupplier())
                        .onClose(() -> {
                            try {
                                headEltBuffer.getDataSupplier().close();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                bodyEltStream = Stream.concat(headItems, bodyItems);
                 * /
        bodyEltStream = debufferedEltStream(headEltBuffer);
        }


        // parse(new SequenceInputStream(Collections.enumeration(Arrays.asList(splitBoundedBodyStream, tailStream))), false);

        // The byte channel must be conditionally bounded

        // HeadItemBuffer must be capped at the split point (this is the case now)
        result = aggregate(isFirstSplit, bodyEltStream, tailElts);

        boolean inspectBufferSizes = false;
        if (inspectBufferSizes) {
        // Load all items into memory and print out the sizes of the byte level buffers
        // The sizes should be relatively small
        try (Stream<T> tmp = result) {
        result = tmp.collect(Collectors.toList()).stream();
        }
        System.out.println("TailByteBuffer.size = " + tailByteBuffer.getBuffer().size());
        //System.out.println("HeadByteBuffer.size = " + headByteBuffer.getBuffer().size());
        }

        // result = aggregate(isFirstSplit, parse(fullStream, false), tailItems);

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


 */