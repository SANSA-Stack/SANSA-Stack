package net.sansa_stack.spark.cli.impl;

import java.util.List;

import org.aksw.jenax.arq.picocli.CmdMixinArq;
import org.aksw.jenax.arq.util.security.ArqSecurity;
import org.aksw.rml.jena.service.RmlSymbols;
import org.apache.hadoop.fs.FileSystem;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.stream.StreamManager;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sys.JenaSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import net.sansa_stack.hadoop.jena.locator.LocatorHdfs;
import net.sansa_stack.query.spark.api.impl.QueryExecBuilder;
import net.sansa_stack.spark.cli.cmd.CmdSansaQuery;
import net.sansa_stack.spark.cli.util.SansaCmdUtils;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;

public class CmdSansaQueryImpl {

    static { JenaSystem.init(); }

    // private static final Logger logger = LoggerFactory.getLogger(CmdSansaQueryImpl.class);

    public static int run(CmdSansaQuery cmd) throws Exception {

        SparkSession sparkSession = SansaCmdUtils.newDefaultSparkSessionBuilder()
                .appName("Sansa Query (" + cmd.queryFiles.size() + " query sources)")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        FileSystem hadoopFs = FileSystem.get(javaSparkContext.hadoopConfiguration());
        StreamManager.get().addLocator(new LocatorHdfs(hadoopFs));

        RddRdfWriterFactory rddRdfWriterFactory = SansaCmdUtils.configureRdfWriter(cmd.outputConfig);

        // CmdMixinArq is serializable
        CmdMixinArq arqConfig = cmd.arqConfig;
        CmdMixinArq.configureGlobal(arqConfig);
        NodeValue.VerboseWarnings = !cmd.hideWarnings;

        // TODO Jena ScriptFunction searches for JavaScript LibFile only searched in the global context
        CmdMixinArq.configureCxt(ARQ.getContext(), arqConfig);
        // Supplier<ExecutionContext> execCxtSupplier = SansaCmdUtils.createExecCxtSupplier(arqConfig);

        String mappingDirectory = cmd.mappingDirectory;

        QueryExecBuilder queryExecBuilder = QueryExecBuilder.newInstance()
            .setSparkSession(sparkSession)
            .addContextMutator(cxt -> {
                CmdMixinArq.configureCxt(cxt, arqConfig);
                cxt
                    // .set(JenaXSymbols.symResourceMgr, new ResourceMgr())
                    .set(ArqSecurity.symAllowFileAccess, true)
                    .set(RmlSymbols.symMappingDirectory, mappingDirectory);
                // .set(RmlSymbols.symD2rqDatabaseResolver, d2rqResolver)
            })
            .setStandardIri(cmd.standardIri)
            .setDagScheduling(cmd.dagScheduling)
            .addFiles(cmd.queryFiles);

        List<Query> queries = queryExecBuilder.getQueries();

        // XXX Could add prefixes collected from queries
        // PrefixMapping prefixes = rddRdfWriterFactory.getGlobalPrefixMapping();

        RDFFormat fmt = rddRdfWriterFactory.getOutputFormat();
        if (fmt == null) {
            // TODO We also need to analyze the insert statements whether they make use of named graphs
            boolean mayProduceQuads = net.sansa_stack.spark.rdd.op.rdf.JavaRddOfBindingsOps.mayQueriesProduceQuads(queries);

            fmt = mayProduceQuads ? RDFFormat.TRIG_BLOCKS : RDFFormat.TURTLE_BLOCKS;
            rddRdfWriterFactory.setOutputFormat(fmt);
        }

        // rddRdfWriterFactory.setUseElephas(true);
        rddRdfWriterFactory.validate();
        // rddRdfWriterFactory.setUseCoalesceOne(true); // for testing
        rddRdfWriterFactory.getPostProcessingSettings().copyFrom(cmd.postProcessConfig);

        RdfSource rdfSource = queryExecBuilder.execToRdf();
        CmdSansaMapImpl.writeOutRdfSources(rdfSource, rddRdfWriterFactory);

        return 0; // exit code
    }
}
