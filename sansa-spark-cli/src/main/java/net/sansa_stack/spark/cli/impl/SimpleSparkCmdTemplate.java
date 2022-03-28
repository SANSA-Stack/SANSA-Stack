package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdMixinSparkInput;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceCollection;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactoryImpl;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public abstract class SimpleSparkCmdTemplate<T>
    implements Callable<T>
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleSparkCmdTemplate.class);

    protected String appName;
    protected List<String> inputFiles;
    protected SparkSession.Builder sparkSessionBuilder;
    protected SparkSession sparkSession;
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

        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        CmdUtils.validatePaths(inputFiles, hadoopConf);

        RdfSourceFactory rdfSourceFactory = RdfSourceFactoryImpl.from(sparkSession);

        rdfSources = CmdUtils.createRdfSourceCollection(rdfSourceFactory, inputFiles, inputSpec);

        StopWatch stopwatch = StopWatch.createStarted();

        process();

        logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

        return null;
    }

    protected abstract void process();
}
