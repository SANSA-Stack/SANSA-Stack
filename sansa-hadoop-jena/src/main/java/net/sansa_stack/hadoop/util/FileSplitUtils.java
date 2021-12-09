package net.sansa_stack.hadoop.util;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import io.reactivex.rxjava3.core.Flowable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class FileSplitUtils {
    /**
     * Util method to open a decoded stream from a split.
     * Useful to read out header information from the first split in order to
     * put it into the hadoop configuration before starting parallel processing.
     *
     * @param split
     * @param job
     * @return
     * @throws IOException
     */
    public static InputStream getDecodedStreamFromSplit(
            FileSplit split,
            Configuration job) throws IOException {
        Path file = split.getPath();

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(file);

        long start = split.getStart();
        long end = start + split.getLength();

        /*
        long maxPrefixesBytes = job.getLong(PARSED_PREFIXES_LENGTH, PARSED_PREFIXES_LENGTH_DEFAULT);
        if (maxPrefixesBytes > end) {
            logger.warn(String.format("Number of bytes set for prefixes parsing (%d) larger than the size of the first" +
                    " split (%d). Could be slow", maxPrefixesBytes, end));
        }
         */
        // end = Math.max(end, maxPrefixesBytes);

        CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);

        InputStream result;
        if (null != codec) {
            Decompressor decompressor = CodecPool.getDecompressor(codec);

            if (codec instanceof SplittableCompressionCodec) {
                SplittableCompressionCodec splitableCodec = (SplittableCompressionCodec) codec;
                result = splitableCodec.createInputStream(fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
            } else {
                result = codec.createInputStream(fileIn, decompressor);
            }
        } else {
            // No reason to bound the stream here
            // new BoundedInputStream(fileIn, split.getLength)
            result = fileIn;
        }

        return result;
    }

    /**
     * Utility method to create a specific number of splits for a file.
     *
     * @param path
     * @param fileLengthTotal
     * @param numSplits
     * @return
     * @throws IOException
     */
    public static Stream<InputSplit> streamFileSplits(org.apache.hadoop.fs.Path path, long fileLengthTotal, long numSplits) throws IOException {
        long splitLength = (long) (fileLengthTotal / (double) numSplits);

        return LongStream.range(0, numSplits)
                .mapToObj(i -> {
                    long start = i * splitLength;
                    long end = Math.min((i + 1) * splitLength, fileLengthTotal);
                    long length = end - start;

                    return (InputSplit) new FileSplit(path, start, length, null);
                });
    }

    public static List<InputSplit> listFileSplits(org.apache.hadoop.fs.Path path, long fileLengthTotal, long numSplits) throws IOException {
        List<InputSplit> result = streamFileSplits(path, fileLengthTotal, numSplits)
                .collect(Collectors.toList());
        return result;
    }

    /** Create a flow of records for a given input split w.r.t. a  given input format */
    public static <T> Flowable<T> createFlow(
            Job job,
            InputFormat<?, T> inputFormat,
            InputSplit inputSplit) {
        return Flowable.generate(() -> {
                    // setup
                    RecordReader<?, T> reader = inputFormat.createRecordReader(inputSplit, new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID()));
                    // initialize
                    reader.initialize(inputSplit, new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID()));
                    return reader;
                },
                (reader, emitter) -> {
                    try {
                        if (reader.nextKeyValue()) {
                            T record = reader.getCurrentValue();
                            emitter.onNext(record);
                        } else {
                            emitter.onComplete();
                        }
                    } catch (Exception e) {
                        emitter.onError(e);
                    }
                },
                AutoCloseable::close);
    }


    /** Util method typically for use with split-related unit tests */
    public static List<Object[]> createTestParameters(Map<String, Range<Integer>> fileToNumSplits) {

        // Post process the map into junit params by enumerating the ranges
        // and creating a test case for each obtained value
        List<Object[]> result = fileToNumSplits.entrySet().stream()
                .flatMap(e -> ContiguousSet.create(e.getValue(), DiscreteDomain.integers()).stream()
                        .map(numSplits -> new Object[]{e.getKey(), numSplits}))
                .collect(Collectors.toList());

        return result;
    }

}
