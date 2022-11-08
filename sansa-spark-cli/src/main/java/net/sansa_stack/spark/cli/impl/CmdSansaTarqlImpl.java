package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.hadoop.format.univocity.conf.UnivocityHadoopConf;
import net.sansa_stack.hadoop.format.univocity.csv.csv.FileInputFormatCsvUnivocity;
import net.sansa_stack.spark.cli.cmd.CmdSansaTarql;
import net.sansa_stack.spark.io.csv.input.CsvDataSources;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSources;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfBindingsOps;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.jena_sparql_api.common.DefaultPrefixes;
import org.aksw.jenax.stmt.core.SparqlStmtMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Called from the Java class [[CmdSansaTarql]]
 */
public class CmdSansaTarqlImpl {
    private static final Logger logger = LoggerFactory.getLogger(CmdSansaTarqlImpl.class);

    public static int run(CmdSansaTarql cmd) throws Exception {
        // Thread.sleep(10000); System.err.println("Done sleping");

        String queryFile = cmd.inputFiles.get(0);
        List<String> csvFiles = cmd.inputFiles.subList(1, cmd.inputFiles.size());
        Query query = SparqlStmtMgr.loadQuery(queryFile, DefaultPrefixes.get());
        logger.info("Loaded query " + query);

        if (!query.isConstructType() && !query.isConstructQuad()) {
            throw new IllegalArgumentException("Query must be of CONSTRUCT type (triples or quads)");
        }

        RddRdfWriterFactory rddRdfWriterFactory = CmdUtils.configureWriter(cmd.outputConfig);
        // rddRdfWriterFactory.setUseElephas(true);

        rddRdfWriterFactory.getPostProcessingSettings().copyFrom(cmd.postProcessConfig);

        SparkSession sparkSession = CmdUtils.newDefaultSparkSessionBuilder()
                .appName("Sansa Tarql (" + cmd.inputFiles + ")")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // Put the CSV options from the CLI into the hadoop context
        Configuration hadoopConf = javaSparkContext.hadoopConfiguration();
        UnivocityHadoopConf univocityConf = new UnivocityHadoopConf();

        DialectMutable csvCliOptions = cmd.csvOptions;
        csvCliOptions.copyInto(univocityConf.getDialect());
        univocityConf.setTabs(cmd.tabs);

        FileInputFormatCsvUnivocity.setUnivocityConfig(hadoopConf, univocityConf);

        JavaRDD<Binding> initialRdd = CmdUtils.createUnionRdd(javaSparkContext, csvFiles,
                input -> CsvDataSources.createRddOfBindings(javaSparkContext, input,
                        univocityConf));

        RdfSource rdfSource;
        if (query.isConstructQuad()) {
            rdfSource = RdfSources.ofQuads(JavaRddOfBindingsOps.tarqlQuads(initialRdd, query));
        } else if (query.isConstructType()) {
            rdfSource = RdfSources.ofTriples(JavaRddOfBindingsOps.tarqlTriples(initialRdd, query));
        } else {
            throw new IllegalArgumentException("Unsupported query type (must be CONSTRUCT): " + query);
        }

        CmdSansaMapImpl.writeOutRdfSources(rdfSource, rddRdfWriterFactory);

        return 0; // exit code
    }
}
