package net.sansa_stack.spark.cli.impl;

import com.google.common.collect.Sets;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import org.aksw.commons.lambda.throwing.ThrowingFunction;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class CmdUtils {
    private static final Logger logger = LoggerFactory.getLogger(CmdSansaTarqlImpl.class);

    public static SparkSession.Builder newDefaultSparkSessionBuilder() {

        SparkSession.Builder result = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryoserializer.buffer.max", "1000") // MB
                .config("spark.kryo.registrator", String.join(", ",
                        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"));

        if (System.getProperty("spark.master") == null) {
            String defaultMaster = "local[*]";
            logger.info("'spark.master' not set - assuming default " + defaultMaster);
            result = result.master(defaultMaster);
        }
        return result;
    }

    public static RddRdfWriterFactory configureWriter(RdfOutputConfig out) {
        PrefixMapping prefixes = new PrefixMappingImpl();

        if (out.getPrefixSources() != null) {
            for (String prefixSource : out.getPrefixSources()) {
                logger.info("Adding prefixes from " + prefixSource);
                Model tmp = RDFDataMgr.loadModel(prefixSource);
                prefixes.setNsPrefixes(tmp);
            }
        }

        RddRdfWriterFactory result = RddRdfWriterFactory.create()
                .setGlobalPrefixMapping(prefixes)
                .setOutputFormat(out.getOutputFormat())
                .setMapQuadsToTriplesForTripleLangs(true)
                // .setAllowOverwriteFiles(true)
                .setPartitionFolder(out.getPartitionFolder())
                .setTargetFile(out.getTargetFile())
                // .setUseElephas(true)
                .setDeletePartitionFolderAfterMerge(true)
                .validate();

        return result;
    }

    public static Set<String> getValidatePaths(Collection<String> paths, Configuration hadoopConf) {

        Set<String> result = paths.stream()
                .map(pathStr -> {
                    Map.Entry<FileSystem, Path> r = null;
                    try {
                        URI uri = new URI(pathStr);
                        // TODO Use try-with-resources for the filesystem?
                        FileSystem fs = FileSystem.get(uri, hadoopConf);
                        Path path = new Path(pathStr);
                        fs.resolvePath(path);
                        r = new AbstractMap.SimpleEntry<>(fs, path);
                    } catch (Exception e) {
                        logger.error(ExceptionUtils.getRootCauseMessage(e));
                    }
                    return r;
                })
                .filter(Objects::nonNull)
                .filter(x -> {
                    boolean isFile = false;
                    try {
                        isFile = x.getKey().isFile(x.getValue());
                    } catch (IOException e) {
                        logger.error(ExceptionUtils.getRootCauseMessage(e));
                    }
                    return isFile;
                })
                .map(Map.Entry::getValue)
                .map(Object::toString)
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

    public static <T> JavaRDD<T> createUnionRdd(
            JavaSparkContext javaSparkContext,
            Collection<String> inputs,
            ThrowingFunction<String, JavaRDD<T>> mapper) throws IOException {

        Configuration configuration = javaSparkContext.hadoopConfiguration();
        validatePaths(inputs, configuration);

        List<JavaRDD<T>> initialRdds = new ArrayList<>();
        for (String path : inputs) {
            try {
                JavaRDD<T> contrib = mapper.apply(path);
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
}
