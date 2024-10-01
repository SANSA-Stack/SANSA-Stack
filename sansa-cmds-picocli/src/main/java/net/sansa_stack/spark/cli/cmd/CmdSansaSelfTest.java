package net.sansa_stack.spark.cli.cmd;


import org.aksw.commons.util.lifecycle.ResourceMgr;
import org.aksw.jenax.arq.picocli.CmdMixinArq;
import org.aksw.jenax.arq.util.quad.DatasetGraphUtils;
import org.aksw.jenax.arq.util.security.ArqSecurity;
import org.aksw.rml.jena.impl.RmlExec;
import org.aksw.rml.jena.impl.RmlToSparqlRewriteBuilder;
import org.aksw.rml.jena.impl.RmlWorkloadOptimizer;
import org.aksw.rml.jena.service.RmlSymbols;
import org.aksw.rmltk.gtfs.GtfsMadridBenchResources;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.DatasetGraph;

import net.sansa_stack.query.spark.api.impl.QueryExecBuilder;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "selftest",
        description = "Runs a series of smaller tests to assess correct functioning of features.",
        mixinStandardHelpOptions = true)
public class CmdSansaSelfTest
        extends CmdBase
        implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        try (ResourceMgr resourceMgr = new ResourceMgr()) {
            String name = "/gtfs-madrid-bench/csv/1";
            Path basePath = ResourceMgr.toPath(resourceMgr, GtfsMadridBenchResources.class, name);
            Path mappingFile = basePath.resolve("mapping.csv.rml.ttl");

            RmlToSparqlRewriteBuilder builder = new RmlToSparqlRewriteBuilder()
                    .addRmlFile(null, mappingFile)
                    .setDenormalize(false)
                    .setDistinct(true)
                    ;

            List<Map.Entry<Query, String>> labeledQueries = builder.generate();
            if (labeledQueries.size() != 86) {
                throw new IllegalStateException("Assertion failed");
                // Assert.assertEquals(86, labeledQueries.size());
            }

            List<Query> queries = RmlWorkloadOptimizer.newInstance()
                    .addSparql(labeledQueries.stream().map(Map.Entry::getKey).toList())
                    .process();

            QueryExecBuilder.newInstance()
                .addQueries(queries);

            // String mappingDirectory = cmd.mappingDirectory;
            String mappingDirectory = basePath.toUri().toString();

            QueryExecBuilder queryExecBuilder = QueryExecBuilder.newInstance()
                // .setSparkSession(sparkSession)
                .addQueries(queries)
                .addContextMutator(cxt -> {
                    // CmdMixinArq.configureCxt(cxt, arqConfig);
                    cxt
                        // .set(JenaXSymbols.symResourceMgr, new ResourceMgr())
                        .set(ArqSecurity.symAllowFileAccess, true)
                        .set(RmlSymbols.symMappingDirectory, mappingDirectory);
                    // .set(RmlSymbols.symD2rqDatabaseResolver, d2rqResolver)
                })
                .setStandardIri(false)
                .setDagScheduling(false)
                ;
                // .addFiles(cmd.queryFiles);

            // RmlExec rmlExec = RmlExec.newBuilder().addQueries(queries).setRmlMappingDirectory(basePath).build();
            // DatasetGraph datasetGraph = rmlExec.toDatasetGraph();
            long tupleCount = queryExecBuilder.execToRdf().asQuads().count();
            // long tupleCount = DatasetGraphUtils.tupleCount(datasetGraph);
            if (tupleCount != 395953) {
                throw new IllegalStateException("Assertion failed");
                // Assert.assertEquals(395953, tupleCount);
            }
        }
        return 0;
    }
}
