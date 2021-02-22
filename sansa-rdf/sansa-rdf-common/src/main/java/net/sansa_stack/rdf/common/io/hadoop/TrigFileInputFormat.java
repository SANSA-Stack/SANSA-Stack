package net.sansa_stack.rdf.common.io.hadoop;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * A Hadoop file input format for Trig RDF files.
 *
 * @author Lorenz Buehmann
 * @author Claus Stadler
 */
public class TrigFileInputFormat
        extends FileInputFormat<LongWritable, Dataset> { // TODO use CombineFileInputFormat?

    private static final Logger logger = LoggerFactory.getLogger(TrigFileInputFormat.class);

    public static final String PREFIXES = "prefixes";
    public static final String PARSED_PREFIXES_LENGTH = "mapreduce.input.trigrecordreader.prefixes.maxlength";
    public static final long PARSED_PREFIXES_LENGTH_DEFAULT = 10 * 1024; // 10KB

    @Override
    public boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        boolean result = codec instanceof SplittableCompressionCodec;
        return result;
    }

    @Override
    public RecordReader<LongWritable, Dataset> createRecordReader(
            InputSplit inputSplit,
            TaskAttemptContext context) {
        if (context.getConfiguration().get(PREFIXES) == null) {
            logger.warn("couldn't get prefixes from Job context");
        }
        return new TrigRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = super.getSplits(job);

        // we use first split and scan for prefixes and base IRI, then pass those to the RecordReader
        // in createRecordReader() method
        if (!splits.isEmpty()) {

            // take first split
            FileSplit firstSplit = (FileSplit) splits.get(0);

            // open input stream from split
            try (InputStream is = getStreamFromSplit(firstSplit, job.getConfiguration())) {

                // Bound the stream for reading out prefixes
                BoundedInputStream boundedIs = new BoundedInputStream(is, PARSED_PREFIXES_LENGTH_DEFAULT);

                // we do two steps here:
                // 1. get all lines with base or prefix declaration
                // 2. use a proper parser on those lines to cover corner case like multiple prefix declarations in a single line
                //      val prefixStr = scala.io.Source.fromInputStream(boundedIs).getLines()
                //        .map(_.trim)
                //        .filterNot(_.isEmpty) // skip empty lines
                //        .filterNot(_.startsWith("#")) // skip comments
                //        .filter(line => line.startsWith("@prefix") || line.startsWith("@base") ||
                //          line.startsWith("prefix") || line.startsWith("base"))
                //        .mkString("\n")

                // https://jena.apache.org/documentation/io/streaming-io.html

                // A Model *isa* PrefixMap; use Model because it can be easily serialized
                Model prefixModel = ModelFactory.createDefaultModel();

                // Create a sink that just tracks nothing but prefixes
                StreamRDF prefixSink = new StreamRDFBase() {
                    @Override
                    public void prefix(String prefix, String iri) {
                        prefixModel.setNsPrefix(prefix, iri);
                    }
                };

                try {
                    RDFDataMgr.parse(prefixSink, boundedIs, Lang.TRIG);
                } catch (Exception e) {
                    // Ignore broken pipe exception because we deliberately cut off the stream
                    // Exception => // logger.warn("TODO Improve this non-fatal exception", e)
                }

                // TODO apparently, prefix declarations could span multiple lines, i.e. technically we
                //  also should consider the next line after a prefix declaration

                // RDFDataMgr.read(dataset, new ByteArrayInputStream(prefixStr.getBytes), Lang.TRIG)
                int prefixCount = prefixModel.getNsPrefixMap().size();
                long prefixByteCount = job.getConfiguration().getLong(PARSED_PREFIXES_LENGTH, PARSED_PREFIXES_LENGTH_DEFAULT);

                logger.info(String.format("Parsed %d prefixes from first %d bytes", prefixCount, prefixByteCount));

                // prefixes are located in default model
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                // Clear any triples - we just want the prefixes
                RDFDataMgr.write(baos, prefixModel, RDFFormat.TURTLE_PRETTY);

                // pass prefix string to job context object
                job.getConfiguration().set("prefixes", baos.toString("UTF-8"));
            }
        }

        return splits;
    }

    public static InputStream getStreamFromSplit(FileSplit split, Configuration job) throws IOException {
        Path file = split.getPath();

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(file);

        long start = split.getStart();
        long end = start + split.getLength();

        long maxPrefixesBytes = job.getLong(PARSED_PREFIXES_LENGTH, PARSED_PREFIXES_LENGTH_DEFAULT);
        if (maxPrefixesBytes > end) {
            logger.warn("Number of bytes set for prefixes parsing ($maxPrefixesBytes) larger than the size of the first" +
                    " split ($end). Could be slow");
        }
        end = Math.max(end, maxPrefixesBytes);

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

}
