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

    public SimpleSparkCmdTemplate(
            String appName,
            List<String> inputFiles) {
        this.appName = appName;
        this.inputFiles = inputFiles;
    }

    protected void initSparkSessionBuilder() {
        this.sparkSessionBuilder = CmdUtils.newDefaultSparkSessionBuilder()
                .appName(appName + "(" + inputFiles + ")");
    }

    protected void finalizeSparkSessionBuilder() {
    }

    protected void processInputs() {
    }

    public T call() throws Exception {

        initSparkSessionBuilder();
        finalizeSparkSessionBuilder();

        sparkSession = sparkSessionBuilder.getOrCreate();

        // Cache spark context and hadoop conf attributes for convenient access
        sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        hadoopConfiguration = sparkContext.hadoopConfiguration();

        CmdUtils.validatePaths(inputFiles, hadoopConfiguration);

        processInputs();

        StopWatch stopwatch = StopWatch.createStarted();

        process();

        logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

        return null;
    }

    protected abstract void process() throws Exception;
}
