package net.sansa_stack.spark.io.rdf.loader;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.aksw.commons.rx.util.RxUtils;
import org.aksw.commons.util.concurrent.CompletionTracker;
import org.aksw.commons.util.concurrent.ExecutorServiceUtils;
import org.aksw.commons.util.ref.Ref;
import org.aksw.commons.util.ref.RefImpl;
import org.aksw.commons.util.ref.RefSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sansa_stack.hadoop.format.jena.base.FileInputFormatRdfBase;
import net.sansa_stack.hadoop.format.jena.trig.FileInputFormatRdfTrigQuad;
import net.sansa_stack.hadoop.format.jena.turtle.FileInputFormatRdfTurtleTriple;
import net.sansa_stack.hadoop.util.FileSplitUtils;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactoryImpl;

/** Async parsing RDF on a single node using hadoop */
public class AsyncRdfParserHadoop {

    private static final Logger logger = LoggerFactory.getLogger(AsyncRdfParserHadoop.class);

    public static class Builder<T>
            implements Cloneable {
        protected Configuration conf;
        protected Path inputFile;
        protected InputFormat<?, T> inputFormat;
        protected RefSupplier<ExecutorService> executorServiceRef;
        protected StreamRDF sink;
        protected BiConsumer<T, StreamRDF> sendRecordToStreamRDF;

        public Builder(Configuration conf, Path inputFile, InputFormat<?, T> inputFormat, RefSupplier<ExecutorService> executorServiceRef, StreamRDF sink, BiConsumer<T, StreamRDF> sendRecordToStreamRDF) {
            this.conf = conf;
            this.inputFile = inputFile;
            this.inputFormat = inputFormat;
            this.executorServiceRef = executorServiceRef;
            this.sink = sink;
            this.sendRecordToStreamRDF = sendRecordToStreamRDF;
        }

        @Override
        public Builder<T> clone() {
            return new Builder<T>(conf, inputFile, inputFormat, executorServiceRef, sink, sendRecordToStreamRDF);
        }

        public Builder<T> setConf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder<T> setInputFile(Path inputFile) {
            this.inputFile = inputFile;
            return this;
        }

        public Builder<T> setSink(StreamRDF sink) {
            this.sink = sink;
            return this;
        }

        public Builder<T> applyDefaults() {
            if (executorServiceRef == null) {
                executorServiceRef = () -> RefImpl.create2(ExecutorServiceUtils.newBlockingThreadPoolExecutor(), null, ExecutorService::shutdownNow);
            }

            return this;
        }

        /*
        public <X> Builder<X> setInputFormat(InputFormat<?, X> inputFormat, BiConsumer<X, StreamRDF> sendRecordToStreamRDF) {
            this.inputFormat = inputFormat;
            this.sendRecordToStreamRDF = sendRecordToStreamRDF;
            return (Builder<X>) this;
        }
        */

        public static <X> Builder<X> create(InputFormat<?, X> inputFormat, BiConsumer<X, StreamRDF> sendRecordToStreamRDF) {
            return new Builder<X>(null, null, inputFormat, null, null, sendRecordToStreamRDF);
        }

        public static Builder<Triple> forTriple() {
            return create(new FileInputFormatRdfTurtleTriple(), (t, s) -> s.triple(t));
        }

        public static Builder<Quad> forQuad() {
            return create(new FileInputFormatRdfTrigQuad(), (q, s) -> s.quad(q));
        }

        public void run() throws Exception {
            Builder<T> clone = clone();
            clone.applyDefaults();

            clone.runActual();
        }

        protected void runActual() throws Exception {
            try (Ref<ExecutorService> ref = executorServiceRef.get()) {
                parseRaw(inputFile, conf, inputFormat, ref.get(), sink, sendRecordToStreamRDF);
            }
        }

        public InputFormat<?, T> getInputFormat() {
            return (InputFormat<?, T>) inputFormat;
        }
    }


    // Create a separate sink for each thread?

    public static void parse(Path file, Configuration conf, StreamRDF sink) throws Exception {
        sink.start();

        FileSystem fileSystem = file.getFileSystem(conf);
        Lang lang = RdfSourceFactoryImpl.probeLang(file, fileSystem);
        if (RDFLanguages.isQuads(lang)) {
            Builder.forQuad().setConf(conf).setInputFile(file).setSink(sink).run();
        } else if (RDFLanguages.isTriples(lang)) {
            Builder.forTriple().setConf(conf).setInputFile(file).setSink(sink).run();
        } else {
            throw new RuntimeException("RDF language is neither quads nor triples " + lang);
        }

        sink.finish();
    }

    /**
     * <b>The sink must be started beforehand!</b>
     *
     * @param inputFile
     * @param conf
     * @param inputFormat
     * @param executorService The Executorservice must be closed externally.
     * @param sink
     * @param sendRecordToStreamRDF
     * @param <T>
     * @throws IOException
     * @throws InterruptedException
     */
    public static <T> void parseRaw(
            Path inputFile,
            Configuration conf,
            // long splitSize, // split size can originate from the conf
            InputFormat<?, T> inputFormat,
            ExecutorService executorService,
            StreamRDF sink,
            BiConsumer<T, StreamRDF> sendRecordToStreamRDF) throws IOException, InterruptedException, ExecutionException {

        /*
        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        conf.set(RecordReaderRdfTrigDataset.RECORD_MAXLENGTH_KEY, "10000");
        conf.set(RecordReaderRdfTrigDataset.RECORD_PROBECOUNT_KEY, "1");
        */


        FileSystem fileSystem = inputFile.getFileSystem(conf);
        // Path inputFile = new Path(inputFileStr);
        FileStatus fileStatus = fileSystem.getFileStatus(inputFile);
        long fileTotalLength = fileStatus.getLen();

        int numCores = Runtime.getRuntime().availableProcessors();
        long splitSize = numCores <= 0 ? 0 : fileTotalLength / numCores;

        if (splitSize < 1000000) {
            splitSize = 1000000;
        }
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Long.toString(splitSize));

        // "mapreduce.input.fileinputformat.split.maxsize"

        Job job = Job.getInstance(conf);

        // add input path of the file
        FileInputFormat.addInputPath(job, inputFile);

        // call once to compute the prefixes
        List<InputSplit> splits = inputFormat.getSplits(job);
        logger.info(String.format("Created %d splits from %s", splits.size(), inputFile));

        if (!splits.isEmpty()) {
            // long splitSize = splits.get(0).getLength();

            PrefixMapping prefixes = FileInputFormatRdfBase.getModel(conf);
            prefixes.getNsPrefixMap().entrySet()
                    .forEach(e -> sink.prefix(e.getKey(), e.getValue()));

            // long numSplits = fileTotalLength / splitSize + Long.signum(fileTotalLength % splitSize);

            // FileSplitUtils.streamFileSplits(inputFile, fileTotalLength, numSplits)
            CompletionTracker completionTracker = CompletionTracker.from(executorService);

            for (InputSplit split : splits) {
                completionTracker.execute(() -> {
                    RxUtils.consume(
                        FileSplitUtils.createFlow(job, inputFormat, split)
                            .map(record -> { sendRecordToStreamRDF.accept(record, sink); return 0; }));
                });
            }

            completionTracker.shutdown();
            completionTracker.awaitTermination();


//            CompletableFuture<?>[] futures = splits.stream()
//                    .map(split ->
//                        CompletableFuture.runAsync(() -> {
//                                    FileSplitUtils.createFlow(job, inputFormat, split)
//                                            .forEach(record -> sendRecordToStreamRDF.accept(record, sink));
//                                }, executorService))
//                    .collect(Collectors.toList())
//                    .toArray(new CompletableFuture[0]);
//
//            CompletableFuture.allOf(futures).get();
        }
    }
}
