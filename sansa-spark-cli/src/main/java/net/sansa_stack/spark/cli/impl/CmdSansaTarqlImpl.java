package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdSansaTarql;
import net.sansa_stack.spark.io.csv.input.CsvDataSources;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfBindingsOps;
import org.aksw.jena_sparql_api.common.DefaultPrefixes;
import org.aksw.jenax.stmt.core.SparqlStmtMgr;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Called from the Java class [[CmdSansaTarql]]
 */
public class CmdSansaTarqlImpl {
    private static final Logger logger = LoggerFactory.getLogger(CmdSansaTarqlImpl.class);

    public static int run(CmdSansaTarql cmd) throws IOException {

        Query query = SparqlStmtMgr.loadQuery(cmd.queryFile, DefaultPrefixes.get());
        logger.info("Loaded query " + query);

        if (!query.isConstructType() && !query.isConstructQuad()) {
            throw new IllegalArgumentException("Query must be of CONSTRUCT type (triples or quads)");
        }

        RddRdfWriterFactory rddRdfWriterFactory = CmdUtils.configureWriter(cmd.outputConfig);

        SparkSession sparkSession = CmdUtils.newDefaultSparkSessionBuilder()
                .appName("Sansa Tarql (" + cmd.inputFiles + ")")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<Binding> initialRdd = CmdUtils.createUnionRdd(javaSparkContext, cmd.inputFiles,
                input -> CsvDataSources.createRddOfBindings(javaSparkContext, input,
                        CSVFormat.EXCEL.builder().setSkipHeaderRecord(true).build()));

        StopWatch stopwatch = StopWatch.createStarted();

        if (query.isConstructQuad()) {
            rddRdfWriterFactory.forQuad(JavaRddOfBindingsOps.tarqlQuads(initialRdd, query)).run();
        } else if (query.isConstructType()) {
            rddRdfWriterFactory.forTriple(JavaRddOfBindingsOps.tarqlTriples(initialRdd, query)).run();
        }

        logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

        return 0; // exit code
    }
}
