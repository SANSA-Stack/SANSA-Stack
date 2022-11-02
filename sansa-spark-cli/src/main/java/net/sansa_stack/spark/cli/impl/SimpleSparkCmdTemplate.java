package net.sansa_stack.spark.cli.impl;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sansa_stack.spark.cli.cmd.CmdMixinSparkInput;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceCollection;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactoryImpl;

public abstract class SimpleSparkCmdTemplate<T>
    implements Callable<T>
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleSparkCmdTemplate.class);

    protected String appName;
    protected List<String> inputFiles;

    protected SparkSession.Builder sparkSessionBuilder;
    protected SparkSession sparkSession;
    protected Configuration hadoopConfiguration;

    protected JavaSparkContext sparkContext;
    protected CmdMixinSparkInput inputSpec;
    protected RdfSourceCollection rdfSources;

    public SimpleSparkCmdTemplate(
            String appName,
            CmdMixinSparkInput inputSpec,
            List<String> inputFiles) {
        this.appName = appName;
        this.inputSpec = inputSpec;
        this.inputFiles = inputFiles;
    }

    protected void initSparkSessionBuilder() {
        this.sparkSessionBuilder = CmdUtils.newDefaultSparkSessionBuilder()
                .appName(appName + "(" + inputFiles + ")");
    }

    protected void finalizeSparkSessionBuilder() {
    }

    public T call() {

        initSparkSessionBuilder();
        finalizeSparkSessionBuilder();

        sparkSession = sparkSessionBuilder.getOrCreate();

        // Cache spark context and hadoop conf attributes for convenient access
        sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        hadoopConfiguration = sparkContext.hadoopConfiguration();

        CmdUtils.validatePaths(inputFiles, hadoopConfiguration);

        RdfSourceFactory rdfSourceFactory = RdfSourceFactoryImpl.from(sparkSession);

        rdfSources = CmdUtils.createRdfSourceCollection(rdfSourceFactory, inputFiles, inputSpec);

        StopWatch stopwatch = StopWatch.createStarted();

        process();

        logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

        return null;
    }

    protected abstract void process();
}
