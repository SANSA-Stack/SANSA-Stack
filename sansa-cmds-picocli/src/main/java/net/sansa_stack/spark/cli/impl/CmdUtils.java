package net.sansa_stack.spark.cli.impl;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.aksw.commons.lambda.serializable.SerializableSupplier;
import org.aksw.commons.lambda.throwing.ThrowingFunction;
import org.aksw.jena_sparql_api.rx.RDFLanguagesEx;
import org.aksw.jenax.arq.picocli.CmdMixinArq;
import org.aksw.jenax.arq.util.exec.ExecutionContextUtils;
import org.aksw.jenax.arq.util.prefix.PrefixMappingTrie;
import org.aksw.jenax.arq.util.security.ArqSecurity;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jena.query.ARQ;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.Context;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.collect.Table.Cell;
import com.google.common.collect.Tables;

import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceCollection;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;

public class CmdUtils {
    private static final Logger logger = LoggerFactory.getLogger(CmdSansaTarqlImpl.class);

    public static final String KRYO_BUFFER_MAX_KEY = "spark.kryo.serializer.buffer.max";

    public static SparkSession.Builder newDefaultSparkSessionBuilder() {

        SparkSession.Builder result = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", String.join(", ",
                        "net.sansa_stack.spark.io.rdf.kryo.JenaKryoRegistrator"));

        String value; // for debugging / inspection
        if ((value = System.getProperty("spark.master")) == null) {
            String defaultMaster = "local[*]";
            logger.info("'spark.master' not set - defaulting to: " + defaultMaster);
            result = result.master(defaultMaster);
        }

        if ((value = System.getProperty(KRYO_BUFFER_MAX_KEY)) == null) {
            result = result.config(KRYO_BUFFER_MAX_KEY, "2048"); // MB
        }
        return result;
    }

    public static RddRdfWriterFactory configureWriter(RdfOutputConfig out) {
        PrefixMapping prefixes = new PrefixMappingTrie();

        if (out.getPrefixSources() != null) {
            for (String prefixSource : out.getPrefixSources()) {
                logger.info("Adding prefixes from " + prefixSource);
                Model tmp = RDFDataMgr.loadModel(prefixSource);
                prefixes.setNsPrefixes(tmp);
            }
        }

        RddRdfWriterFactory result = RddRdfWriterFactory.create();

        // Try to derive the output format from the file name (if given)
        if (out.getOutputFormat() != null) {
            result = result.setOutputFormat(out.getOutputFormat());
        }

        RDFFormat fmt = result.getOutputFormat();
        if (fmt == null) {
            String fileName = out.getTargetFile();
            Lang lang = RDFDataMgr.determineLang(fileName, null, null);
            if (lang != null) {
                fmt = StreamRDFWriter.defaultSerialization(lang);

                result = result.setOutputFormat(fmt);
            }
        }

        result = result
                .setGlobalPrefixMapping(prefixes)
                .setMapQuadsToTriplesForTripleLangs(true)
                .setDeferOutputForUsedPrefixes(out.getPrefixOutputDeferCount())
                // .setAllowOverwriteFiles(true)
                .setPartitionFolder(out.getPartitionFolder())
                .setTargetFile(out.getTargetFile())
                // .setUseElephas(true)
                .setDeletePartitionFolderAfterMerge(true)
                .setAllowOverwriteFiles(out.isOverwriteAllowed());
                //.validate();

        return result;
    }

    public static Set<String> getValidatePaths(Collection<String> paths, Configuration hadoopConf) {

        Set<String> result = paths.stream()
                .map(pathStr -> {
                    Cell<String, FileSystem, Path> r = null;
                    try {
                        URI uri = new URI(pathStr);
                        // TODO Use try-with-resources for the filesystem?
                        FileSystem fs = FileSystem.get(uri, hadoopConf);
                        Path path = new Path(pathStr);
                        path = fs.resolvePath(path);
                        r = Tables.immutableCell(pathStr, fs, path);
                    } catch (Exception e) {
                        logger.error(ExceptionUtils.getRootCauseMessage(e));
                    }
                    return r;
                })
                .filter(Objects::nonNull)
                .filter(cell -> {
                    boolean isFile = false;
                    try {
                        isFile = cell.getColumnKey().getFileStatus(cell.getValue()).isFile();
                    } catch (IOException e) {
                        logger.error(ExceptionUtils.getRootCauseMessage(e));
                    }
                    return isFile;
                })
                .map(Cell::getRowKey)
                .collect(Collectors.toSet());

        return result;
    }

    public static void validatePaths(Collection<String> paths, Configuration hadoopConf) {
        Set<String> validPathStrs = CmdUtils.getValidatePaths(paths, hadoopConf);
        Set<String> inputSet = new LinkedHashSet<>(paths);

        Set<String> invalidPaths = Sets.difference(inputSet, validPathStrs);
        if (!invalidPaths.isEmpty()) {
            throw new IllegalArgumentException("The following paths are invalid (do not exist or are not a (readable) file): " + invalidPaths);
        }
    }

    public static RdfSourceCollection createRdfSourceCollection(RdfSourceFactory rdfSourceFactory,
                                                                Collection<String> inputs,
                                                                RdfInputConfig inputConfig) {

        String inputFormatStr = inputConfig.getInputFormat();
        Lang lang = inputFormatStr == null ? null : RDFLanguagesEx.findLang(inputFormatStr);
        if (inputFormatStr != null && lang == null) {
            throw new IllegalArgumentException("Unknown input format: " + inputFormatStr);
        }

        RdfSourceCollection result = rdfSourceFactory.newRdfSourceCollection();
        for (String input : inputs) {
            if (lang == null) {
                lang = RDFLanguages.contentTypeToLang(RDFLanguages.guessContentType(input));
            }

            RdfSource rdfSource = rdfSourceFactory.get(input, lang);
            result.add(rdfSource);
        }

        return result;
    }

    public static <T> JavaRDD<T> createUnionRdd(
            JavaSparkContext javaSparkContext,
            Collection<String> inputs,
            ThrowingFunction<String, JavaRDD<T>> mapper) {
        return createUnionRdd(javaSparkContext, inputs, in -> in, mapper);
    }

    /**
     * Only creates a union rdd from the given collection of input objects if all
     * paths obtained from the input via the 'inputToPath' function are accessible.
     */
    public static <T, X> JavaRDD<T> createUnionRdd(
            JavaSparkContext javaSparkContext,
            Collection<X> inputs,
            Function<? super X, String> inputToPath,
            ThrowingFunction<? super X, JavaRDD<T>> mapper) {

        List<String> paths = inputs.stream().map(inputToPath).collect(Collectors.toList());

        Configuration configuration = javaSparkContext.hadoopConfiguration();
        validatePaths(paths, configuration);

        List<JavaRDD<T>> initialRdds = new ArrayList<>();
        for (X input : inputs) {
            try {
                JavaRDD<T> contrib = mapper.apply(input);
                initialRdds.add(contrib);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        JavaRDD<T> result;
        if (initialRdds.size() == 1) {
            result = initialRdds.get(0);
        } else {
            result = javaSparkContext.union(initialRdds.toArray(new JavaRDD[0]));
        }
        return result;
    }

    public static Supplier<ExecutionContext> createExecCxtSupplier(CmdMixinArq arqConfig) {
        SerializableSupplier<ExecutionContext> execCxtSupplier = () -> {
            Context baseCxt = ARQ.getContext().copy();
            CmdMixinArq.configureCxt(baseCxt, arqConfig);
            baseCxt.set(ArqSecurity.symAllowFileAccess, true);

            // Scripting can only be set via system property
            // baseCxt.setTrue(ARQ.systemPropertyScripting)

            ExecutionContext execCxt = ExecutionContextUtils.createExecCxtEmptyDsg(baseCxt);
            return execCxt;
        };
        return execCxtSupplier;
    }
}
