package net.sansa_stack.hadoop.format.jena.base;

import net.sansa_stack.hadoop.util.FileSplitUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.EltStreamRDF;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

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
        extends FileInputFormat<LongWritable, T>
        implements CanParseRdf { // TODO use CombineFileInputFormat?

    private static final Logger logger = LoggerFactory.getLogger(FileInputFormatRdfBase.class);

    public static final String PREFIXES_KEY = "prefixes";
    public static final String BASE_IRI_KEY = "base";

    // A prefix.cc dump with 2500 prefxes required 153KB, so 1MB should be plenty
    // TODO We could add another property which governs abort of looking for prefixes
    //  if there is none within 'n' number of seen parsing events (triples, quads)
    public static final long PARSED_PREFIXES_LENGTH_DEFAULT = 1 * 1024 * 1024;

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

    /**
     *
     * @param prefixModel If null then a default model will be generated
     * @param inSupp An input stream supplier. taken stream will be closed.
     * @param lang The RDF language. Must not be null.
     * @param limit
     * @return
     * @throws Exception
     */
    public static Model readPrefixesIntoModel(Model prefixModel, Callable<InputStream> inSupp, Lang lang, Long limit) {
        try (InputStream in = inSupp.call()) {
            Model result = readPrefixesIntoModel(prefixModel, in, lang, limit);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Model readPrefixes(Callable<InputStream> inSupp, Configuration conf) {
        long limit = getPrefixByteCount(conf);
        Model result = readPrefixesIntoModel(null, inSupp, lang, limit);
        return result;
    }

    /** Public method to parse prefixes w.r.t. this input format configuration */
    @Override
    public Model parsePrefixes(InputStream in, Configuration conf) {
        long limit = getPrefixByteCount(conf);
        Model result = readPrefixesIntoModel(null, in, lang, limit);
        return result;
    }

    /**
     * At present this method actually reads the full model - so be sure
     * to only supply a bounded input stream
     * I need to add a PR to JENA to open up its AsyncParser API and this method should then use it
     */
    public static Model readPrefixesIntoModel(Model sink, InputStream in, Lang lang, Long limit) {
        if (limit != null && limit >= 0) {
            in = new BoundedInputStream(in, limit);
        }
        return readPrefixesIntoModel(sink, in, lang);
    }

    public long getPrefixByteCount(Configuration conf) {
        // open input stream from split
        long result = prefixesLengthMaxKey == null
                ? 0l
                : conf.getLong(prefixesLengthMaxKey, PARSED_PREFIXES_LENGTH_DEFAULT);
        return result;
    }

    public static Model readPrefixesIntoModel(Model sink, InputStream in, Lang lang) {
        // A Model *isa* PrefixMap; use Model because it can be easily serialized
        Model dst = sink == null
                ? ModelFactory.createDefaultModel()
                : sink;

        // Create a sink that just tracks nothing but prefixes
        StreamRDF prefixSink = new StreamRDFBase() {
            @Override
            public void prefix(String prefix, String iri) {
                dst.setNsPrefix(prefix, iri);
            }
        };

        try(Stream<EltStreamRDF> stream = AsyncParser.of(in, lang, null).streamElements()){
            Iterator<EltStreamRDF> it = stream.iterator();
            long nonPrefixEventCount = 0;
            long maxNonPrefixEventCount = 1000;
            while (it.hasNext() && nonPrefixEventCount < maxNonPrefixEventCount) {
                EltStreamRDF event = it.next();
                if (event.isPrefix()) {
                    prefixSink.prefix(event.getPrefix(), event.getIri());
                    nonPrefixEventCount = 0;
                } else {
                    ++nonPrefixEventCount;
                }
            }

            // RDFDataMgr.parse(prefixSink, in, lang);

            // Only retain prefixes
            dst.removeAll();
        } catch (Exception e) {
            // Ignore broken pipe exception because we deliberately cut off the stream
            // Exception => // logger.warn("TODO Improve this non-fatal exception", e)
        }

        return dst;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        Configuration conf = job.getConfiguration();

        List<InputSplit> splits = super.getSplits(job);

        // we use first split and scan for prefixes and base IRI, then pass those to the RecordReader
        // in createRecordReader() method
        if (!splits.isEmpty()) {

            // take first split
            FileSplit firstSplit = (FileSplit) splits.get(0);

            long prefixByteCount = getPrefixByteCount(conf);

            // Use the decoded stream for reading in prefixes
            Model prefixModel = readPrefixes(
                    () -> FileSplitUtils.getDecodedStreamFromSplit(firstSplit, conf),
                    job.getConfiguration());

            // TODO apparently, prefix declarations could span multiple lines, i.e. technically we
            //  also should consider the next line after a prefix declaration

            int prefixCount = prefixModel.getNsPrefixMap().size();

            logger.info(String.format("Parsed %d prefixes from first %d bytes", prefixCount, prefixByteCount));

            // prefixes are located in default model
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // Clear any triples - we just want the prefixes
            RDFDataMgr.write(baos, prefixModel, RDFFormat.TURTLE_PRETTY);

            // pass prefix string to job context object
            conf.set(BASE_IRI_KEY, firstSplit.getPath().toString());
            conf.set(PREFIXES_KEY, baos.toString("UTF-8"));
        }

        return splits;
    }

    /** Extract a Model from a hadoop conf using {@link FileInputFormatRdfBase#PREFIXES_KEY} */
    public static Model getModel(Configuration conf) {
        return getModel(conf, FileInputFormatRdfBase.PREFIXES_KEY);
    }

    /** Extract a Model from a hadoop conf. Result is never null;
     *  empty if there was no entry for the key or exception on parse error. */
    public static Model getModel(Configuration conf, String key) {
        String str = conf.get(key);
        // Model *isa* PrefixMapping
        Model result = ModelFactory.createDefaultModel();
        if (str != null) {
            RDFDataMgr.read(result, new StringReader(str), null, Lang.TURTLE);
        }
        return result;
    }
}
