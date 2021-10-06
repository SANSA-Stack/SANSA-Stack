package net.sansa_stack.hadoop.jena.rdf.base;

import net.sansa_stack.hadoop.util.FileSplitUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
 * Base class for unit testing of reading an RDF file with
 * an arbitrary number of splits.
 * RDF is read as Datasets which means that triples are expanded to quads.
 * <p>
 * (We could generalize to tuples of RDF terms using Bindings)
 * <p>
 * The only method that need to be overriden is {@link #createRecordReaderActual(InputSplit, TaskAttemptContext)}.
 *
 * @param <T>
 */
public abstract class FileInputFormatRdfBase<T>
        extends FileInputFormat<LongWritable, T> { // TODO use CombineFileInputFormat?

    private static final Logger logger = LoggerFactory.getLogger(FileInputFormatRdfBase.class);

    public static final String PREFIXES_KEY = "prefixes";
    public static final String BASE_IRI_KEY = "base";
    public static final long PARSED_PREFIXES_LENGTH_DEFAULT = 10 * 1024; // 10KB

    /**
     * Input language
     */
    protected Lang lang;
    protected String prefixesLengthMaxKey;

    public FileInputFormatRdfBase(Lang lang, String prefixesLengthMaxKey) {
        this.lang = lang;
        this.prefixesLengthMaxKey = prefixesLengthMaxKey;
    }

    @Override
    public boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        // If there is no codec - indicated by codec equals null - then the input is considered to be splittable
        boolean result = codec == null || codec instanceof SplittableCompressionCodec;
        return result;
    }

    @Override
    public final RecordReader<LongWritable, T> createRecordReader(
            InputSplit inputSplit,
            TaskAttemptContext context) {
        if (context.getConfiguration().get(PREFIXES_KEY) == null) {
            logger.warn("couldn't get prefixes from Job context");
        }

        return createRecordReaderActual(inputSplit, context);
    }

    public abstract RecordReader<LongWritable, T> createRecordReaderActual(
            InputSplit inputSplit,
            TaskAttemptContext context);


    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = super.getSplits(job);

        // we use first split and scan for prefixes and base IRI, then pass those to the RecordReader
        // in createRecordReader() method
        if (!splits.isEmpty()) {

            // take first split
            FileSplit firstSplit = (FileSplit) splits.get(0);

            // open input stream from split
            try (InputStream is = FileSplitUtils.getDecodedStreamFromSplit(firstSplit, job.getConfiguration())) {

                // Bound the decoded stream for reading out prefixes
                BoundedInputStream boundedIs = new BoundedInputStream(is, PARSED_PREFIXES_LENGTH_DEFAULT);

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
                    RDFDataMgr.parse(prefixSink, boundedIs, lang);
                } catch (Exception e) {
                    // Ignore broken pipe exception because we deliberately cut off the stream
                    // Exception => // logger.warn("TODO Improve this non-fatal exception", e)
                }

                // TODO apparently, prefix declarations could span multiple lines, i.e. technically we
                //  also should consider the next line after a prefix declaration

                // RDFDataMgr.read(dataset, new ByteArrayInputStream(prefixStr.getBytes), Lang.TRIG)
                int prefixCount = prefixModel.getNsPrefixMap().size();
                long prefixByteCount = job.getConfiguration().getLong(prefixesLengthMaxKey, PARSED_PREFIXES_LENGTH_DEFAULT);

                logger.info(String.format("Parsed %d prefixes from first %d bytes", prefixCount, prefixByteCount));

                // prefixes are located in default model
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                // Clear any triples - we just want the prefixes
                RDFDataMgr.write(baos, prefixModel, RDFFormat.TURTLE_PRETTY);

                // pass prefix string to job context object
                Configuration conf = job.getConfiguration();
                conf.set(BASE_IRI_KEY, firstSplit.getPath().toString());
                conf.set(PREFIXES_KEY, baos.toString("UTF-8"));
            }
        }

        return splits;
    }
}
