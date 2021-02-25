package net.sansa_stack.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
     * @param testHadoopPath
     * @param fileLengthTotal
     * @param numSplits
     * @return
     * @throws IOException
     */
    public static List<InputSplit> generateFileSplits(org.apache.hadoop.fs.Path testHadoopPath, long fileLengthTotal, int numSplits) throws IOException {
        int splitLength = (int) (fileLengthTotal / (double) numSplits);

        List<InputSplit> result = IntStream.range(0, numSplits)
                .mapToObj(i -> {
                    long start = i * splitLength;
                    long end = Math.min((i + 1) * splitLength, fileLengthTotal);
                    long length = end - start;

                    return (InputSplit) new FileSplit(testHadoopPath, start, length, null);
                })
                .collect(Collectors.toList());
        return result;
    }

}
