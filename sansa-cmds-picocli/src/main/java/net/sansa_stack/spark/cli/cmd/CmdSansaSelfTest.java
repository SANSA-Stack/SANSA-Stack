package net.sansa_stack.spark.cli.cmd;

import org.aksw.commons.io.util.FileUtils;
import org.aksw.commons.util.lifecycle.ResourceMgr;
import org.aksw.jenax.arq.picocli.CmdMixinArq;
import org.aksw.jenax.arq.util.exec.query.JenaXSymbols;
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

import java.io.IOException;
import java.nio.file.Files;
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
            Path internalBasePath = ResourceMgr.toPath(resourceMgr, GtfsMadridBenchResources.class, name);

            Path basePath = Files.createTempDirectory("sansa");
            String basePathStr = basePath.toString();
            try {
                // IMPORTANT NOTE: Passing the java.nio.Path to hadoop turns out to be quite complicated.
                // For this reason the class path resources for the self-test are unpacked to the local filesystem.
                FileUtils.copyDirectory(internalBasePath, basePath);

                Path mappingFile = basePath.resolve("mapping.csv.rml.ttl");

                RmlToSparqlRewriteBuilder builder = new RmlToSparqlRewriteBuilder()
                        .addRmlFile(null, mappingFile)
                        .setDenormalize(false)
                        .setDistinct(true);

                List<Map.Entry<Query, String>> labeledQueries = builder.generate();
                if (labeledQueries.size() != 86) {
                    throw new IllegalStateException("Assertion failed");
                }

                List<Query> queries = RmlWorkloadOptimizer.newInstance()
                        .addSparql(labeledQueries.stream().map(Map.Entry::getKey).toList())
                        .process();

                QueryExecBuilder.newInstance()
                    .addQueries(queries);

                QueryExecBuilder queryExecBuilder = QueryExecBuilder.newInstance()
                    // .setSparkSession(sparkSession)
                    .addQueries(queries)
                    .addContextMutator(cxt -> {
                        ResourceMgr subResMgr = new ResourceMgr();
                        // CmdMixinArq.configureCxt(cxt, arqConfig);
                        cxt
                            // .set(JenaXSymbols.symResourceMgr, new ResourceMgr())
                            .set(ArqSecurity.symAllowFileAccess, true)
                            .set(RmlSymbols.symMappingDirectory, Path.of(basePathStr))
                            .set(JenaXSymbols.symResourceMgr, subResMgr);
                            // .set(RmlSymbols.symD2rqDatabaseResolver, d2rqResolver)
                    })
                    .addContextCloser(cxt -> {
                        ResourceMgr subResMgr = cxt.get(JenaXSymbols.symResourceMgr);
                        subResMgr.close();
                    })
                    .setStandardIri(false)
                    .setDagScheduling(false);
                    // .addFiles(cmd.queryFiles);

                long tupleCount = queryExecBuilder.execToRdf().asQuads().count();
                if (tupleCount != 395953) {
                    throw new IllegalStateException("Assertion failed");
                }
            } finally {
                FileUtils.deleteRecursivelyIfExists(basePath);
            }
        }
        return 0;
    }
}
