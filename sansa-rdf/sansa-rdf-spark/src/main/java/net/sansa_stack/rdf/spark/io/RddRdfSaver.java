package net.sansa_stack.rdf.spark.io;


import org.aksw.commons.io.util.FileMerger;
import org.aksw.commons.io.util.FileUtils;
import org.aksw.commons.lambda.serializable.SerializableBiConsumer;
import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.aksw.commons.lambda.throwing.ThrowingFunction;
import org.aksw.jena_sparql_api.rx.RDFLanguagesEx;
import org.aksw.jena_sparql_api.utils.io.StreamRDFUtils;
import org.aksw.jena_sparql_api.utils.io.WriterStreamRDFBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.jena.graph.Triple;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.hadoop.rdf.types.TripleWritable;
import org.apache.jena.query.Dataset;
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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A fluent API for configuration of how to save an RDD of RDF data to disk.
 * This class uniformly handles Triples, Quads, Model, Datasets, etc using a set of
 * lambdas for relevant conversion.
 *
 * Instances of this class should be created using the appropriate createFor[Type] methods.
 *   TODO Implementation for Model is currently missing
 *
 * @param <T>
 */
public class RddRdfSaver<T> {
    private static final Logger logger = LoggerFactory.getLogger(RddRdfSaver.class);

    protected JavaRDD<T> rdd;
    protected Path partitionFolder;
    protected Path targetFile;

    protected boolean useCoalesceOne;

    /* Only applicable if a on outputFile is specified */
    protected boolean deletePartitionFolderAfterMerge;
    protected PrefixMapping globalPrefixMapping;
    protected RDFFormat outputFormat;

    // protected Object saveMode; // SaveMode saveMode;//mode: io.SaveMode.Value = SaveMode.ErrorIfExists,
    protected boolean allowOverwriteFiles;
    protected boolean useElephas;

    protected boolean partitionsAsIndependentFiles;

    /** Whether to convert quads to triples if a triple-based output format is requested */
    protected boolean mapQuadsToTriplesForTripleLangs;

    // Static bindings for the generic 'T'
    protected BiConsumer<T, StreamRDF> sendRecordToStreamRDF;
    protected Function<JavaRDD<T>, JavaRDD<Triple>> convertToTriple;
    protected Function<JavaRDD<T>, JavaRDD<Quad>> convertToQuad;

    // Cached attributes from the given RDD
    protected JavaSparkContext sparkContext;
    protected Configuration hadoopConfiguration;

    public RddRdfSaver(
            JavaRDD<T> rdd,
            BiConsumer<T, StreamRDF> sendRecordToStreamRDF,
            Function<JavaRDD<T>, JavaRDD<Triple>> convertToTriple,
            Function<JavaRDD<T>, JavaRDD<Quad>> convertToQuad) {
        super();
        this.rdd = rdd;

        this.sparkContext = JavaSparkContext.fromSparkContext(rdd.context());
        this.hadoopConfiguration = sparkContext.hadoopConfiguration();

        this.sendRecordToStreamRDF = sendRecordToStreamRDF;
        this.convertToTriple = convertToTriple;
        this.convertToQuad = convertToQuad;
    }

    public boolean isMapQuadsToTriplesForTripleLangs() {
        return mapQuadsToTriplesForTripleLangs;
    }

    /**
     * Whether to convert quads to triples if a triple-based output format is requested
     * Jena by default discards any quad outside of the default graph when writing to a triple format.
     * Setting this flag to true will map each quad in a named graph to the default graph.
     */
    public RddRdfSaver setMapQuadsToTriplesForTripleLangs(boolean mapQuadsToTriplesForTripleLangs) {
        this.mapQuadsToTriplesForTripleLangs = mapQuadsToTriplesForTripleLangs;
        return this;
    }

    public boolean isUseCoalesceOne() {
        return useCoalesceOne;
    }

    public void setUseCoalesceOne(boolean useCoalesceOne) {
        this.useCoalesceOne = useCoalesceOne;
    }

    public boolean isDeletePartitionFolderAfterMerge() {
        return deletePartitionFolderAfterMerge;
    }

    public RddRdfSaver<T> setDeletePartitionFolderAfterMerge(boolean deletePartitionFolderAfterMerge) {
        this.deletePartitionFolderAfterMerge = deletePartitionFolderAfterMerge;
        return this;
    }

    public PrefixMapping getGlobalPrefixMapping() {
        return globalPrefixMapping;
    }

    public Path getPartitionFolder() {
        return partitionFolder;
    }

    public RddRdfSaver<T> setPartitionFolder(Path partitionFolder) {
        this.partitionFolder = partitionFolder;
        return this;
    }

    public RddRdfSaver<T> setPartitionFolder(String partitionFolder) {
        return setPartitionFolder(partitionFolder == null ? null : new Path(partitionFolder));
    }

    public Path getTargetFile() {
        return targetFile;
    }

    public RddRdfSaver<T> setTargetFile(Path targetFile) {
        this.targetFile = targetFile;
        return this;
    }

    public RddRdfSaver<T> setTargetFile(String targetFile) {
        return setTargetFile(targetFile == null ? null : new Path(targetFile));
    }

    /**
     * Set a prefix mapping to be used "globally" across all partitions.
     *
     * @param globalPrefixMapping
     * @return
     */
    public RddRdfSaver<T> setGlobalPrefixMapping(PrefixMapping globalPrefixMapping) {
        this.globalPrefixMapping = globalPrefixMapping;
        return this;
    }

    public RDFFormat getOutputFormat() {
        return outputFormat;
    }

    public RddRdfSaver<T> setOutputFormat(RDFFormat outputFormat) {
        this.outputFormat = outputFormat;
        return this;
    }

    public boolean isAllowOverwriteFiles() {
        return allowOverwriteFiles;
    }

    public RddRdfSaver<T> setAllowOverwriteFiles(boolean allowOverwriteFiles) {
        this.allowOverwriteFiles = allowOverwriteFiles;
        return this;
    }

    public boolean isUseElephas() {
        return useElephas;
    }

    public RddRdfSaver<T> setUseElephas(boolean useElephas) {
        this.useElephas = useElephas;
        return this;
    }

    public boolean isPartitionsAsIndependentFiles() {
        return partitionsAsIndependentFiles;
    }

    public RddRdfSaver<T> setPartitionsAsIndependentFiles(boolean partitionsAsIndependentFiles) {
        this.partitionsAsIndependentFiles = partitionsAsIndependentFiles;
        return this;
    }

    /**
     * Pass this object to a consumer. Useful to conditionally configure this object
     * without breaking the fluent chain:
     *
     * <pre>
     *    rdd.configureSave().mutate(self -> { if (condition) { self.setX(); }}).run();
     * </pre>
     *
     * @param action
     * @return
     */
    public RddRdfSaver<T> mutate(Consumer<RddRdfSaver<T>> action) {
        action.accept(this);
        return this;
    }


    /**
     * Run the save action according to configuration
     *
     * @throws IOException
     */
    public void run() throws IOException {
/*
        val outFilePath = Paths.get(outFile).toAbsolutePath
        val outFileFileName = outFilePath.getFileName.toString
        val outFolderPath =
        if (outFolder == null) outFilePath.resolveSibling(outFileFileName + "-parts")
        else Paths.get(outFolder).toAbsolutePath

        saveToFolder(outFolderPath.toString, prefixMapping, rdfFormat, mode, exitOnError)
        mergeFolder(outFilePath, outFolderPath, "part*")
*/
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
                logger.info(String.format("Removing existing file/folder: %s", effPartitionFolder));
                partitionFolderFs.delete(effPartitionFolder, true);
            } else {
                throw new IllegalArgumentException("Folder already exists: " + effPartitionFolder);
            }
        }

        URI targetFileUri = null;
        FileSystem targetFileFs = null;
        if (targetFile != null) {
            targetFileFs = targetFile.getFileSystem(hadoopConfiguration);
            if (targetFileFs.exists(targetFile)) {
                if (allowOverwriteFiles) {
                    logger.info(String.format("Removing existing file: %s", targetFile));
                    targetFileFs.delete(targetFile, false);
                } else {
                    throw new IllegalArgumentException("File already exists: " + targetFile);
                }
            }
        }


        JavaRDD<T> effectiveRdd = rdd;

        if (useCoalesceOne) {
            effectiveRdd = effectiveRdd.coalesce(1);
        }

        if (useElephas) {
            Lang lang = RDFLanguages.filenameToLang(effPartitionFolder.toString());
            Objects.requireNonNull(String.format("Could not determine language from path %s ", effPartitionFolder));

            if (RDFLanguages.isTriples(lang)) {
                JavaRDD<Triple> triples = convertToTriple.apply(effectiveRdd);
                saveUsingElephas(triples, effPartitionFolder, lang, TripleWritable::new);
            } else if (RDFLanguages.isQuads(lang)) {
                JavaRDD<Quad> quads = convertToQuad.apply(effectiveRdd);
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

            saveToFolder(effectiveRdd, effPartitionFolder.toString(), outputFormat, mapQuadsToTriplesForTripleLangs, pmap, this.sendRecordToStreamRDF);
        }

        if (targetFile != null) {
            if (!(partitionFolderFs instanceof LocalFileSystem) || !(targetFileFs instanceof LocalFileSystem)) {
                throw new IllegalArgumentException("Merge currently only supports local file system");
            } else {
                java.nio.file.Path nioFolder = Paths.get(effPartitionFolder.toString());
                java.nio.file.Path nioFile = Paths.get(targetFile.toString());
                mergeFolder(nioFile, nioFolder, "part-*", null);
            }

            if (deletePartitionFolderAfterMerge) {
                logger.info(String.format("Removing temporary output folder: %s", effPartitionFolder));
                partitionFolderFs.delete(effPartitionFolder, true);
            }
        }
    }

    public static void validateOutFolder(Path path, Configuration conf, boolean deleteIfExists) throws IOException {
        // Path fsPath = new Path(path);
        FileSystem fs = FileSystem.get(path.toUri(), conf);

        if (fs.exists(path)) {
            if (deleteIfExists) {
                fs.delete(path, true);
            } else {
                throw new IllegalArgumentException("File already exists: " + fs);
            }
        }
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


    // TODO This method should go to a common util class
    public static void mergeFolder(
            java.nio.file.Path outFile,
            java.nio.file.Path srcFolder,
            String pattern,
            Comparator<? super java.nio.file.Path> pathComparator) throws IOException {
        if (pathComparator == null) {
            pathComparator = (java.nio.file.Path a, java.nio.file.Path b) -> a.getFileName().toString().compareTo(b.getFileName().toString());
        }
        List<java.nio.file.Path> partPaths = FileUtils.listPaths(srcFolder, pattern);
        Collections.sort(partPaths, pathComparator);
        logger.info(String.format("Creating file %s by merging %d files from %s",
                outFile, partPaths.size(), srcFolder));

        // val sw = Stopwatch.createStarted
        FileMerger merger = FileMerger.create(outFile, partPaths);
        merger.addProgressListener(self -> logger.info(
                String.format("Write progress for %s: %.2f%%",
                        outFile.getFileName(),
                        self.getProgress() * 100.0)));
        merger.run();
    }

    /**
     * Save the RDD to a single file.
     * Underneath invokes [[JenaDatasetWriter#saveToFolder]] and merges
     * the set of files created by it.
     * See [[JenaDatasetWriter#saveToFolder]] for supported formats.
     *
     * @param mode
     * @param exitOnError /
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
    public static Iterator<String> partitionMapperNTriples(Iterator<Triple> it) {
        return WrappedIterator.create(it).mapWith(FmtUtils::stringForTriple);
    }

    public static Iterator<String> partitionMapperNQuads(Iterator<Quad> it) {
        return WrappedIterator.create(it).mapWith(FmtUtils::stringForQuad);
    }

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
                    PrefixMap pm = WriterStreamRDFBaseUtils.getPrefixMap(tmp);
                    for (Map.Entry<String, String> e : prefixMapping.getNsPrefixMap().entrySet()) {
                        pm.add(e.getKey(), e.getValue());
                    }

                    rawWriter = StreamRDFUtils.wrapWithoutPrefixDelegation(rawWriter);
                }
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
     * @param mode the expected behavior of saving the data to a data source
     */
    public static <T> void saveToFolder(
            JavaRDD<T> javaRdd,
            String path,
            RDFFormat rdfFormat,
            boolean mapQuadsToTriplesForTripleLangs,
            PrefixMapping globalPrefixMapping,
            BiConsumer<T, StreamRDF> sendRecordToStreamRDF) throws IOException {

        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(javaRdd.context());

        String prefixStr = globalPrefixMapping != null && !globalPrefixMapping.hasNoMappings()
                ? toString(globalPrefixMapping, RDFFormat.TURTLE_PRETTY)
                : null;

        Broadcast<PrefixMapping> prefixMappingBc = sparkContext.broadcast(globalPrefixMapping);

        String rdfFormatStr = rdfFormat.toString();

        JavaRDD<String> dataBlocks = javaRdd.mapPartitions(it -> {
            RDFFormat rdfFmt = RDFLanguagesEx.findRdfFormat(rdfFormatStr);
            PrefixMapping pmap = prefixMappingBc.getValue();
            Function<OutputStream, StreamRDF> streamRDFFactory = createStreamRDFFactory(rdfFmt, mapQuadsToTriplesForTripleLangs, pmap);

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

    public static <T> void saveUsingElephas(
            JavaRDD<T> rdd,
            Path path,
            Lang lang,
            SerializableFunction<? super T, ?> recordToWritable) {

        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(rdd.context());
        Configuration hadoopConfiguration = sparkContext.hadoopConfiguration();

        OutputFormatRdfRegistry.FormatEntry entry = OutputFormatRdfRegistry.getInstance().get(lang);
        Objects.requireNonNull(entry, String.format("No format registered for %s", lang));
        // TODO Add some registry to connect rdd + rdfFormat with the
        // hadoop API
        // HadoopRdfIORegistry.createQuadReader()
        //val sc = quads.sparkContext

        JavaPairRDD<?, ?> pairRdd = rdd
                .mapToPair(v -> new Tuple2<>(new LongWritable(0), recordToWritable.apply(v)));

        pairRdd.saveAsNewAPIHadoopFile(path.toString(),
                entry.getKeyClass(),
                entry.getValueClass(),
                entry.getOutputFormatClass(),
                hadoopConfiguration);
    }


    /**
     * Create method.
     * Note that the 'sendRecordToSTreamRDF' parameter is serializable
     *
     * @param rdd
     * @param sendRecordToStreamRDF
     * @param convertToTriple
     * @param convertToQuad
     * @param <T>
     * @return
     */
    public static <T> RddRdfSaver create(
            JavaRDD<T> rdd,
            SerializableBiConsumer<T, StreamRDF> sendRecordToStreamRDF,
            Function<JavaRDD<T>, JavaRDD<Triple>> convertToTriple,
            Function<JavaRDD<T>, JavaRDD<Quad>> convertToQuad) {
        return new RddRdfSaver<>(
                rdd, sendRecordToStreamRDF, convertToTriple, convertToQuad);
    }

    public static RddRdfSaver<Triple> createForTriple(JavaRDD<Triple> rdd) {
        return create(rdd,
                (triple, streamRDF) -> streamRDF.triple(triple),
                x -> x,
                x -> x.map(triple -> Quad.create(Quad.defaultGraphNodeGenerated, triple)));
    }

    public static RddRdfSaver<Quad> createForQuad(JavaRDD<Quad> rdd) {
        return create(rdd,
                (quad, streamRDF) -> streamRDF.quad(quad),
                x -> x.map(Quad::asTriple),
                x -> x);
    }

    public static RddRdfSaver<Dataset> createForDataset(JavaRDD<Dataset> rdd) {
        return RddRdfSaver.<Dataset>create(rdd,
                (ds, streamRDF) -> StreamRDFOps.sendDatasetToStream(ds.asDatasetGraph(), streamRDF),
                x -> x.flatMap(ds -> WrappedIterator.create(ds.asDatasetGraph().find()).mapWith(Quad::asTriple)),
                x -> x.flatMap(ds -> ds.asDatasetGraph().find())
        );
    }
}
