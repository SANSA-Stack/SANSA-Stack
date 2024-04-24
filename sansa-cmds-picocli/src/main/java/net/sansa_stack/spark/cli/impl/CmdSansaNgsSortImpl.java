package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdSansaNgsSort;
import net.sansa_stack.spark.cli.util.SansaCmdUtils;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactories;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfDatasetsOps;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Sort / distinctify a collection of named graphs.
 * A single named graph corresponds to a record and must thus fit into memory
 *
 */
public class CmdSansaNgsSortImpl {
    private static final Logger logger = LoggerFactory.getLogger(CmdSansaNgsSortImpl.class);

    public static int run(CmdSansaNgsSort cmd) throws IOException {

        List<String> inputStrs = cmd.inputFiles;

        SparkSession sparkSession = SansaCmdUtils.newDefaultSparkSessionBuilder()
                .appName("Sansa NGS Sort (" + cmd.inputFiles + ")")
                .getOrCreate();

        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();

        RddRdfWriterFactory rddRdfWriterFactory = SansaCmdUtils.configureRdfWriter(cmd.outputConfig);
        rddRdfWriterFactory.validate();

        SansaCmdUtils.validatePaths(inputStrs, hadoopConf);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        RdfSourceFactory rdfSourceFactory = RdfSourceFactories.of(sparkSession);
        List<JavaRDD<DatasetOneNg>> sources = new ArrayList<>();
        for (String input : cmd.inputFiles) {

            RdfSource rdfSource = rdfSourceFactory.get(input);
            // Lang lang = rdfSource.getLang();
            sources.add(rdfSource.asDatasets().toJavaRDD());
        }

        StopWatch stopwatch = StopWatch.createStarted();

        JavaRDD<DatasetOneNg> rdd = javaSparkContext.union(sources.toArray(new JavaRDD[0]));
        rdd = JavaRddOfDatasetsOps.groupNamedGraphsByGraphIri(rdd, cmd.sort, cmd.distinct, cmd.numPartitions);

        rddRdfWriterFactory.forDataset(rdd).run();

        logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

        return 0; // exit code
    }
}
