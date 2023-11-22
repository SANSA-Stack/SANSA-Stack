package net.sansa_stack.spark.io.rdf.output;

import net.sansa_stack.spark.io.common.HadoopOutputFormat;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOps;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import org.aksw.commons.io.util.FileMerger;
import org.aksw.commons.io.util.FileUtils;
import org.aksw.commons.lambda.serializable.SerializableBiConsumer;
import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.aksw.commons.lambda.serializable.SerializableSupplier;
import org.aksw.commons.lambda.throwing.ThrowingFunction;
import org.aksw.jenax.arq.analytics.NodeAnalytics;
import org.aksw.jenax.arq.dataset.api.DatasetGraphOneNg;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.aksw.jenax.arq.util.lang.RDFLanguagesEx;
import org.aksw.jenax.arq.util.prefix.PrefixMapAdapter;
import org.aksw.jenax.arq.util.prefix.PrefixMappingTrie;
import org.aksw.jenax.arq.util.streamrdf.StreamRDFDeferred;
import org.aksw.jenax.arq.util.streamrdf.StreamRDFUtils;
import org.aksw.jenax.arq.util.streamrdf.WriterStreamRDFBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.hadoop.rdf.types.TripleWritable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.*;
import org.apache.jena.riot.writer.WriterStreamRDFBase;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.FmtUtils;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <b>Important</b>: Instances of this class should only be created using {@link RddRdfWriterFactory} because the
 * factory is RDD-independent and can validate settings at an early stage.
 *
 * <p />
 *
 * This class implements a fluent API for configuration of how to save an RDD of RDF data to disk.
 * This class uniformly handles Triples, Quads, Model, Datasets, etc using a set of
 * lambdas for relevant conversion.
 *
 * Instances of this class should be created using the appropriate createFor[Type] methods.
 *
 * @param <T>
 */
public class RddRdfWriter<T>
    extends RddRdfWriterSettings<RddRdfWriter<T>>
{
    private static final Logger logger = LoggerFactory.getLogger(RddRdfWriter.class);

    /** References the lambdas in RddRdfOpsImpl directly (saves one entry in the call stack per record) */
    protected RddRdfOpsImpl<T> dispatcher;

    // Cached attributes from the given RDD
    protected JavaSparkContext sparkContext;

    protected JavaRDD<? extends T> rdd;

    protected Configuration hadoopConfiguration;

    public RddRdfWriter(RddRdfOpsImpl<T> dispatcher) {
        super();
        this.dispatcher = dispatcher;
    }

    public RddRdfWriter<T> setRdd(JavaRDD<? extends T> rdd) {
        this.rdd = rdd;

        this.sparkContext = rdd == null ? null : JavaSparkContext.fromSparkContext(rdd.context());
        this.hadoopConfiguration = rdd == null ? null : sparkContext.hadoopConfiguration();

        return this;
    }

    public JavaRDD<? extends T> getRdd() {
        return rdd;
    }

    // @Override
    // protected RddRdfWriter<T> self() {
        // return (RddRdfWriter<T>)this;
    // }

    /** Same as {@link #run()} but without the checked IOException */
    public void runUnchecked() {
        try {
            run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // @Override
    public void run() throws IOException {
        if (isConsoleOutput()) {
            runOutputToConsole();
        } else {
            runSpark();
        }
    }

    /**
     * Create the effective RDD w.r.t. configuration (sort, unqiue, optimize prefixes)
     * If optimize prefixes is enabled then invoking this method will immediately perform that analysis
     * The current behavior is that this writer's prefix map will be updated to the used prefixes.
     * However, this is subject to change such that a new writer instance with the used prefixes
     * is created.
     */
    public JavaRDD<T> getEffectiveRdd(RdfPostProcessingSettings settings) {
        JavaRDD<T> result = rdd.map(x -> (T)x);

        if (settings != null) {

            if (Boolean.TRUE.equals(settings.getDistinct())) {
                Integer n = settings.getDistinctPartitions();
                result = n == null
                        ? result.distinct()
                        : result.distinct(n);
            }

            if (Boolean.TRUE.equals(settings.getSort())) {
                boolean isAscending = Boolean.TRUE.equals(settings.getSortAscending());
                int numPartitions = Optional.ofNullable(settings.getSortPartitions())
                        .orElse(rdd.getNumPartitions());

                result = result.sortBy(dispatcher.getKeyFunction()::apply, isAscending, numPartitions);
            }

            if (Boolean.TRUE.equals(settings.getOptimizePrefixes())) {
                result = result.cache();
                PrefixMapping declaredPrefixes = getGlobalPrefixMapping();
                if (!declaredPrefixes.getNsPrefixMap().isEmpty()) {
                    Map<String, String> usedPm = JavaRddOps.aggregateUsingJavaCollector(
                            dispatcher.convertToNode(result),
                            NodeAnalytics.usedPrefixes(declaredPrefixes.getNsPrefixMap()).asCollector());
                    setGlobalPrefixMapping(usedPm);
                }
            }
        }

        return result;
    }

    protected void runOutputToConsole() throws IOException {
        try (OutputStream out = consoleOutSupplier.get()) {

            // val out = Files.newOutputStream(Paths.get("output.trig"),
            // StandardOpenOption.WRITE, StandardOpenOption.CREATE)
            // System.out
            StreamRDF coreWriter = StreamRDFWriter.getWriterStream(out, outputFormat, null);

            if (coreWriter instanceof WriterStreamRDFBase) {
                WriterStreamRDFBaseUtils.setNodeToLabel((WriterStreamRDFBase) coreWriter,
                        SyntaxLabels.createNodeToLabelAsGiven());
            }

            StreamRDF writer = new StreamRDFDeferred(coreWriter, true, globalPrefixMapping, deferOutputForUsedPrefixes,
                    Long.MAX_VALUE, null);

            writer.start();
            StreamRDFOps.sendPrefixesToStream(globalPrefixMapping, writer);

            // val it = effectiveRdd.collect
            Iterator<? extends T> it = rdd.toLocalIterator();
            it.forEachRemaining(item -> {
                dispatcher.sendRecordToStreamRDF.accept(item, writer);
            });
            writer.finish();
            out.flush();
        }
    }

    public void runActual(RddWriterSettings<?> cxt) throws IOException {
        JavaRDD effectiveRdd = getEffectiveRdd(postProcessingSettings);

        if (useCoalesceOne) {
            effectiveRdd = effectiveRdd.coalesce(1);
        }

        Path effPartitionFolder = cxt.getPartitionFolder();


        boolean useElephas = true;
        boolean useOldElephas = false;

        if (useElephas) {
            RddRdfWriter2 writer =  new RddRdfWriter2(outputFormat, mapQuadsToTriplesForTripleLangs, globalPrefixMapping);
            Lang lang = outputFormat.getLang();

            if (RDFLanguages.isTriples(lang)) {
                JavaRDD<Triple> triples = dispatcher.convertToTriple.apply(effectiveRdd);
                writer.writeTriples(triples.rdd(), effPartitionFolder);
            } else if (RDFLanguages.isQuads(lang)) {
                JavaRDD<Quad> quads = dispatcher.convertToQuad.apply(effectiveRdd);
                writer.writeQuads(quads.rdd(), effPartitionFolder);
            } else {
                throw new IllegalStateException(String.format("Language %s is neiter triples nor quads", lang));
            }

        } else if (useOldElephas) {
            Lang lang = RDFLanguages.filenameToLang(effPartitionFolder.toString());
            Objects.requireNonNull(String.format("Could not determine language from path %s ", effPartitionFolder));

            if (RDFLanguages.isTriples(lang)) {
                JavaRDD<Triple> triples = dispatcher.convertToTriple.apply(effectiveRdd);
                saveUsingElephas(triples, effPartitionFolder, lang, TripleWritable::new);
            } else if (RDFLanguages.isQuads(lang)) {
                JavaRDD<Quad> quads = dispatcher.convertToQuad.apply(effectiveRdd);
                saveUsingElephas(quads, effPartitionFolder, lang, QuadWritable::new);
            } else {
                throw new IllegalStateException(String.format("Language %s is neiter triples nor quads", lang));
            }
        } else {
            // FIXME Clarify semantics of partitions as independentFile
            //   Even if the flag is true then there is still the question whether to...
            //     use deferred output (per partition) in order to collect prefixes
            //     allow extension of prefixes
            PrefixMapping pmap = isPartitionsAsIndependentFiles() ? null : globalPrefixMapping;

            saveToFolder(effectiveRdd, effPartitionFolder.toString(), outputFormat, mapQuadsToTriplesForTripleLangs, pmap, this.dispatcher.sendRecordToStreamRDF);
        }
    }

    /**
     * Run the save action according to configuration
     *
     * @throws IOException
     */
    public void runSpark() throws IOException {
        RddWriterSettings<?> effectiveSettings = RddWriterUtils.prepare(this, JavaSparkContextUtils.fromRdd(rdd).hadoopConfiguration());
        // OutputContext cxt = prepareOutputContext(effectiveSettings);
        runActual(effectiveSettings);
        RddWriterUtils.postProcess(effectiveSettings);
    }

    /**
     * Convert a prefix mapping to a string
     */
    public static String toString(PrefixMapping prefixMapping, RDFFormat rdfFormat) {
        Model tmp = ModelFactory.createDefaultModel();
        tmp.setNsPrefixes(prefixMapping);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RDFDataMgr.write(baos, tmp, RDFFormat.TURTLE_PRETTY);
        String result = null;
        try {
            result = baos.toString("UTF-8").trim();
        } catch (UnsupportedEncodingException e) {
            // Should never happen
            throw new RuntimeException(e);
        }
        return result;
    }

    public static Iterator<String> partitionMapperNTriples(Iterator<Triple> it) {
        return WrappedIterator.create(it).mapWith(FmtUtils::stringForTriple);
    }

    public static Iterator<String> partitionMapperNQuads(Iterator<Quad> it) {
        return WrappedIterator.create(it).mapWith(FmtUtils::stringForQuad);
    }

    /**
     * Create a function that can create a StreamRDF instance that is backed by the given
     * OutputStream.
     *
     * @param rdfFormat
     * @param prefixMapping
     * @return
     */
    public static Function<OutputStream, StreamRDF> createStreamRDFFactory(
            RDFFormat rdfFormat,
            boolean mapQuadsToTriplesForTripleLangs,
            PrefixMapping prefixMapping) {

        return out -> {

            StreamRDF rawWriter = StreamRDFWriter.getWriterStream(out, rdfFormat, null);

            StreamRDF coreWriter = StreamRDFUtils.unwrap(rawWriter);

            // Retain blank nodes as given
            if (coreWriter instanceof WriterStreamRDFBase) {
                WriterStreamRDFBase tmp = (WriterStreamRDFBase)coreWriter;
                WriterStreamRDFBaseUtils.setNodeToLabel(tmp, SyntaxLabels.createNodeToLabelAsGiven());

                if (prefixMapping != null) {
                    // Inject the trie-based prefix mapping rather than using the default
                    WriterStreamRDFBaseUtils.setPrefixMap(tmp, new PrefixMapAdapter(prefixMapping));
                    /*
                    PrefixMap pm = WriterStreamRDFBaseUtils.getPrefixMap(tmp);
                    for (Map.Entry<String, String> e : prefixMapping.getNsPrefixMap().entrySet()) {
                        pm.add(e.getKey(), e.getValue());
                    }
                    */

                    rawWriter = StreamRDFUtils.wrapWithoutPrefixDelegation(rawWriter);
                }
                WriterStreamRDFBaseUtils.updateFormatter(tmp);
            }

            if (RDFLanguages.isTriples(rdfFormat.getLang()) && mapQuadsToTriplesForTripleLangs) {
                rawWriter = new StreamRDFWrapper(rawWriter) {
                    @Override
                    public void quad(Quad quad) {
                        super.triple(quad.asTriple());
                    }
                };
            }

            return rawWriter;
        };
    }

    public static <T> ThrowingFunction<Iterator<T>, Iterator<String>> partitionMapperRDFStream(
            Function<OutputStream, StreamRDF> streamRDFFactory,
            BiConsumer<? super T, StreamRDF> sendRecordToWriter) {

        // Look up the string here in order to avoid having to serialize the RDFFormat object
        // RDFFormat rdfFormat = RDFLanguagesEx.findRdfFormat(rdfFormatStr);
        return it -> {
            Iterator<String> r;
            if (it.hasNext()) {

                PipedOutputStream out = new PipedOutputStream();
                PipedInputStream in = new PipedInputStream(out, 8 * 1024);

                // Set the writer's prefix map without writing them out
                StreamRDF writer = streamRDFFactory.apply(out);
                Thread thread = new Thread(() -> {
                    try {
                        writer.start();
                        while (it.hasNext()) {
                            T record = it.next();
                            sendRecordToWriter.accept(record, writer);
                            // StreamRDFOps.sendDatasetToStream(ds.asDatasetGraph(), writer);
                        }
                        writer.finish();
                        out.flush();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        // IOUtils.closeQuietly(out, null);
                        try {
                            out.close();
                        } catch (Exception e) {
                            logger.warn("Failed to close a stream", e);
                        }
                    }
                });

                thread.start();

                // Collections.singleton(baos.toString("UTF-8").trim).iterator().asScala
                r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                        .lines().iterator();
            } else {
                r = Collections.emptyIterator();
            }
            return r;
        };
    }

    /**
     * Save the data in Trig/Turtle or its sub-formats (n-quads/n-triples) format.
     * If prefixes should be written out then they have to provided as an argument to
     * the prefixMapping parameter.
     * Prefix mappings are broadcasted to and processed in a .mapPartition operation.
     * If the prefixMapping is non-empty then the first part file written out contains them.
     * No other partition will write out prefixes.
     *
     * @param path the folder into which the file(s) will be written to
     * mode the expected behavior of saving the data to a data source
     */
    @Deprecated
    public static <T> void saveToFolder(
            JavaRDD<T> javaRdd,
            String path,
            RDFFormat rdfFormat,
            boolean mapQuadsToTriplesForTripleLangs,
            PrefixMapping globalPrefixMapping,
            BiConsumer<T, StreamRDF> sendRecordToStreamRDF) throws IOException {


        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(javaRdd.context());

        Lang lang = rdfFormat.getLang();

        boolean isPrefixesSupported = !Lang.NTRIPLES.equals(lang);
        boolean isTurtleOrTrig = Arrays.asList(Lang.TURTLE, Lang.TRIG).contains(lang);

        // TODO Prefixes must generally be handled via StreamRDF
        String prefixStr = null;

        if (isPrefixesSupported && globalPrefixMapping != null && !globalPrefixMapping.hasNoMappings()) {
            if (isTurtleOrTrig) {
                prefixStr = toString(globalPrefixMapping, RDFFormat.TURTLE_PRETTY);
            }
        }

        Broadcast<PrefixMapping> prefixMappingBc = sparkContext.broadcast(globalPrefixMapping);

        String rdfFormatStr = rdfFormat.toString();

        JavaRDD<String> dataBlocks = javaRdd.mapPartitions(it -> {
            RDFFormat rdfFmt = RDFLanguagesEx.findRdfFormat(rdfFormatStr);
            PrefixMapping rawPmap = prefixMappingBc.getValue();

            // Ensure a trie-backed prefix mapping in order to handle large amounts of prefixes
            PrefixMapping triePmap = new PrefixMappingTrie();
            triePmap.setNsPrefixes(rawPmap);

            Function<OutputStream, StreamRDF> streamRDFFactory = createStreamRDFFactory(rdfFmt, mapQuadsToTriplesForTripleLangs, triePmap);

            ThrowingFunction<Iterator<T>, Iterator<String>> mapper = partitionMapperRDFStream(
                    streamRDFFactory, sendRecordToStreamRDF);
            Iterator<String> r = mapper.apply(it);
            return r;
        });

        // If there are prefixes then serialize them into their own partition and prepend them to all
        // the other serialized data partitions.
        // Note that this feature is unstable as it relies on spark retaining order of partitions (which so far it does)

        if (prefixStr != null) {
            JavaRDD<String> prefixRdd = sparkContext.parallelize(Collections.singletonList(prefixStr));
            dataBlocks = prefixRdd.union(dataBlocks);
        }
        dataBlocks.saveAsTextFile(path);
    }

    public static <T> void saveUsingElephas(
            JavaRDD<T> rdd,
            Path path,
            Lang lang,
            SerializableFunction<? super T, ?> recordToWritable) {

        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(rdd.context());
        Configuration hadoopConfiguration = sparkContext.hadoopConfiguration();

        HadoopOutputFormat entry = RddRdfWriterFormatRegistry.getInstance().get(lang);
        Objects.requireNonNull(entry, String.format("No format registered for %s", lang));
        // TODO Add some registry to connect rdd + rdfFormat with the
        // hadoop API
        // HadoopRdfIORegistry.createQuadReader()
        //val sc = quads.sparkContext

        JavaPairRDD<?, ?> pairRdd = rdd
                .mapToPair(v -> new Tuple2<>(new LongWritable(0), recordToWritable.apply(v)));

        RddWriterUtils.save(pairRdd, entry, path, hadoopConfiguration);
    }

    public static RddRdfWriter<Triple> createForTriple() {
        return new RddRdfWriter<>(RddRdfOpsImpl.createForTriple());
    }

    public static RddRdfWriter<Quad> createForQuad() {
        return new RddRdfWriter<>(RddRdfOpsImpl.createForQuad());
    }

    public static RddRdfWriter<Graph> createForGraph() {
        return new RddRdfWriter<>(RddRdfOpsImpl.createForGraph());
    }

    public static RddRdfWriter<DatasetGraphOneNg> createForDatasetGraph() {
        return new RddRdfWriter<>(RddRdfOpsImpl.createForDatasetGraph());
    }

    public static RddRdfWriter<Model> createForModel() {
        return new RddRdfWriter<>(RddRdfOpsImpl.createForModel());
    }

    public static RddRdfWriter<DatasetOneNg> createForDataset() {
        return new RddRdfWriter<>(RddRdfOpsImpl.createForDataset());
    }

    public static void validate(RddRdfWriterSettings<?> settings) {
        RDFFormat outputFormat = settings.getOutputFormat();

        if (!StreamRDFWriter.registered(outputFormat)) {
            throw new IllegalArgumentException(outputFormat + " is not a streaming format");
        }

        // TODO We need access to the hadoop FileSystem object for further validation
//        if (!settings.isAllowOverwriteFiles()) {
//             Path targetFile = settings.getTargetFile();

//            if (Files.exists(targetFile, LinkOption.NOFOLLOW_LINKS)) {
//                throw new IllegalArgumentException("File already exists and overwrite is prohibited: " + targetFile);
//            }
//        }
    }

    public static <T> void sendToStreamRDF(
            JavaRDD<T> javaRdd,
            SerializableBiConsumer<T, StreamRDF> sendRecordToStreamRDF,
            SerializableSupplier<StreamRDF> streamRdfSupplier) {

        javaRdd.foreachPartition(it -> {
            StreamRDF streamRdf = streamRdfSupplier.get();
            streamRdf.start();
            while (it.hasNext()) {
                T item = it.next();
                sendRecordToStreamRDF.accept(item, streamRdf);
            }
            streamRdf.finish();
        });
    }
}



/*
    public OutputContext prepareOutputContext() throws IOException {

/ *
        val outFilePath = Paths.get(outFile).toAbsolutePath
        val outFileFileName = outFilePath.getFileName.toString
        val outFolderPath =
        if (outFolder == null) outFilePath.resolveSibling(outFileFileName + "-parts")
        else Paths.get(outFolder).toAbsolutePath

        saveToFolder(outFolderPath.toString, prefixMapping, rdfFormat, mode, exitOnError)
        mergeFolder(outFilePath, outFolderPath, "part*")
* /

        Path effPartitionFolder = partitionFolder;

        if (targetFile != null && partitionFolder == null) {
            targetFile.getFileSystem(hadoopConfiguration);
            // java.nio.file.Path targetFilePath = Paths.get(targetFile.toUri());
            String targetFileName = targetFile.getName();
            // java.nio.file.Path effPartitionFolderPath = path.resolveSibling(targetFileName + "-parts");
            effPartitionFolder = new Path(targetFile.toUri().resolve("part-" + targetFileName));
            // effPartitionFolder = new Path(effPartitionFolderPath.toString());
        }


        FileSystem partitionFolderFs = effPartitionFolder.getFileSystem(hadoopConfiguration);
        if (partitionFolderFs.exists(effPartitionFolder)) {
            if (allowOverwriteFiles) {
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("Attempting to safely remove existing file/folder: %s", effPartitionFolder));
                }
                // partitionFolderFs.delete(effPartitionFolder, true);
                safeDeletePartitionFolder(partitionFolderFs, effPartitionFolder, hadoopConfiguration);

                if (partitionFolderFs.exists(effPartitionFolder)) {
                    String msg = String.format("Could not safely remove partition folder '%s' because non-hadoop files exist. Please delete manually.", effPartitionFolder);
//                    if (logger.isInfoEnabled()) {
//                        logger.info(msg);
//                    }
                    throw new RuntimeException(msg);
                }
            } else {
                throw new IllegalArgumentException("Folder already exists: " + effPartitionFolder);
            }
        }

        // URI targetFileUri = null;
        FileSystem targetFileFs = null;
        if (targetFile != null) {
            targetFileFs = targetFile.getFileSystem(hadoopConfiguration);
            if (targetFileFs.exists(targetFile)) {
                if (allowOverwriteFiles) {
                    if (logger.isInfoEnabled()) {
                        logger.info(String.format("Removing existing file: %s", targetFile));
                    }
                    targetFileFs.delete(targetFile, false);
                } else {
                    throw new IllegalArgumentException("File already exists: " + targetFile);
                }
            }
        }


        // JavaRDD<T> effectiveRdd = rdd.map(x -> (T)x);
        JavaRDD effectiveRdd = getEffectiveRdd(postProcessingSettings);

        if (useCoalesceOne) {
            effectiveRdd = effectiveRdd.coalesce(1);
        }
        return new OutputContext(effectiveRdd, targetFileFs, targetFile, partitionFolderFs, effPartitionFolder);
    }

    public void postProcess(OutputContext cxt) throws IOException {

        Path targetFile = cxt.targetFile;
        if (targetFile != null) {
            if (!(cxt.partitionFolderFs instanceof LocalFileSystem) || !(cxt.targetFileFs instanceof LocalFileSystem)) {
                throw new IllegalArgumentException("Merge currently only supports local file system");
            } else {
                java.nio.file.Path nioFolder = Paths.get(cxt.effPartitionFolder.toString());
                java.nio.file.Path nioFile = Paths.get(targetFile.toString());
                mergeFolder(nioFile, nioFolder, "part-*", null);
            }

            if (deletePartitionFolderAfterMerge) {
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("Removing temporary output folder: %s", cxt.effPartitionFolder));
                }
                cxt.partitionFolderFs.delete(cxt.effPartitionFolder, true);
            }
        }
    }
*/
//    public static void saveUsingElephasQuad(JavaRDD<Quad> rddOfQuad, Path path) {
//        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(rddOfQuad.context());
//        Configuration hadoopConfiguration = sparkContext.hadoopConfiguration();
//
//        Lang lang = RDFLanguages.filenameToLang(path.toString());
//
//        // unknown format
//        if (!RDFLanguages.isQuads(lang)) {
//            throw new IllegalArgumentException(
//                    String.format("Couldn't determine syntax for RDF quads based on file extension in given path %s", path.toString()));
//        }
//
//        OutputFormatRdfRegistry.Entry entry = OutputFormatRdfRegistry.find(lang);
//        Objects.requireNonNull(entry, String.format("No OutputFormat registered for language %s", lang));
//
//        JavaPairRDD<?, QuadWritable> pairRdd = rddOfQuad.mapToPair(v -> new Tuple2<>(new LongWritable(0), new QuadWritable(v)));
//        pairRdd.saveAsNewAPIHadoopFile(path,
//                entry.getKeyClass(),
//                entry.getValueClass(),
//                entry.getOutputFormatClass(),
//                hadoopConfiguration);
//    }
    /*
        public static Iterator<String> partitionMapperRDFStream(
                Iterator<Dataset> it,
                RDFFormat rdfFormat,
                PrefixMapping prefixMapping) throws IOException {
            return partitionMapperRDFStream(it, rdfFormat, prefixMapping,
                    (ds, s) -> StreamRDFOps.sendDatasetToStream(ds.asDatasetGraph(), s));
        }
    */

/**
 * Save the RDD to a single file.
 * Underneath invokes [[JenaDatasetWriter#saveToFolder]] and merges
 * the set of files created by it.
 * See [[JenaDatasetWriter#saveToFolder]] for supported formats.
 *
 * mode
 * exitOnError /
 *                    def saveToFile(outFile: String,
 *                    prefixMapping: PrefixMapping,
 *                    rdfFormat: RDFFormat,
 *                    outFolder: String,
 *                    mode: io.SaveMode.Value = SaveMode.ErrorIfExists,
 *                    exitOnError: Boolean = false): Unit = {
 *                    <p>
 *                    val outFilePath = Paths.get(outFile).toAbsolutePath
 *                    val outFileFileName = outFilePath.getFileName.toString
 *                    val outFolderPath =
 *                    if (outFolder == null) outFilePath.resolveSibling(outFileFileName + "-parts")
 *                    else Paths.get(outFolder).toAbsolutePath
 *                    <p>
 *                    saveToFolder(outFolderPath.toString, prefixMapping, rdfFormat, mode, exitOnError)
 *                    mergeFolder(outFilePath, outFolderPath, "part*")
 *                    }
 */

// public static void partitionMapper
