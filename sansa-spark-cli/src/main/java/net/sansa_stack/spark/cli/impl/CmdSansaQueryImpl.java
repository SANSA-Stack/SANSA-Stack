package net.sansa_stack.spark.cli.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import net.sansa_stack.spark.cli.cmd.CmdSansaQuery;
import net.sansa_stack.spark.cli.cmd.CmdSansaTarql;
import net.sansa_stack.spark.io.csv.input.CsvDataSources;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSources;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import net.sansa_stack.query.spark.rdd.op.JavaRddOfBindingsOps;
import org.aksw.commons.collections.IterableUtils;
import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;
import org.aksw.commons.model.csvw.univocity.UnivocityCsvwConf;
import org.aksw.jena_sparql_api.rx.script.SparqlScriptProcessor;
import org.aksw.jena_sparql_api.sparql.ext.url.E_IriAsGiven;
import org.aksw.jenax.stmt.core.SparqlStmt;
import org.aksw.jenax.stmt.core.SparqlStmtQuery;
import org.aksw.jenax.stmt.core.SparqlStmtUpdate;
import org.aksw.jenax.stmt.util.SparqlStmtUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryType;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sys.JenaSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class CmdSansaQueryImpl {

    static { JenaSystem.init(); }

    private static final Logger logger = LoggerFactory.getLogger(CmdSansaTarqlImpl.class);

    public static int run(CmdSansaQuery cmd) throws Exception {

        RddRdfWriterFactory rddRdfWriterFactory = CmdUtils.configureWriter(cmd.outputConfig);
        PrefixMapping prefixes = rddRdfWriterFactory.getGlobalPrefixMapping();

        // TODO Add support to read query files from HDFS
        SparqlScriptProcessor processor = SparqlScriptProcessor.createPlain(null, null);
        processor.process(cmd.queryFiles);
        List<SparqlStmt> stmts = processor.getPlainSparqlStmts();

        List<Query> queries = stmts.stream().map(SparqlStmt::getQuery).collect(Collectors.toList());

        Set<QueryType> queryTypes = queries.stream().map(Query::queryType).collect(Collectors.toSet());
        QueryType queryType = IterableUtils.expectOneItem(queryTypes); // TODO Add error that mixing query types is not possible


//        PrefixMap usedPrefixes = PrefixMapFactory.create();
//        for (SparqlStmt stmt : stmts) {
//            // TODO optimizePrefixes should not modify in-place because it desyncs with the stmts's original string
//            SparqlStmtUtils.optimizePrefixes(stmt);
//            PrefixMapping pm = stmt.getPrefixMapping();
//            if (pm != null) {
//                usedPrefixes.putAll(pm);
//            }
//        }

        if (cmd.useIriAsGiven) {
            stmts = stmts.stream()
                    .map(stmt -> SparqlStmtUtils.applyElementTransform(stmt, E_IriAsGiven.ExprTransformIriToIriAsGiven::transformElt))
                    .collect(Collectors.toList());
        }

        RDFFormat fmt = rddRdfWriterFactory.getOutputFormat();
        if (fmt == null) {
            // TODO We also need to analyze the insert statements whether they make use of named graphs
            boolean mayProduceQuads = net.sansa_stack.spark.rdd.op.rdf.JavaRddOfBindingsOps.mayProduceQuads(stmts);

            fmt = mayProduceQuads ? RDFFormat.TRIG_BLOCKS : RDFFormat.TURTLE_BLOCKS;
            rddRdfWriterFactory.setOutputFormat(fmt);
        }

        Lang outLang = fmt.getLang();

        rddRdfWriterFactory.setUseElephas(true);
        rddRdfWriterFactory.validate();
        rddRdfWriterFactory.getPostProcessingSettings().copyFrom(cmd.postProcessConfig);

        SparkSession sparkSession = CmdUtils.newDefaultSparkSessionBuilder()
                .appName("Sansa Query (" + queries.size() + " queries)")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<Binding> initialRdd = JavaRddOfBindingsOps.unitRdd(javaSparkContext);

        switch (queryType) {
            case CONSTRUCT: {
                JavaRDD<Quad> rdd = JavaRddOfBindingsOps.execSparqlConstruct(initialRdd, queries, null);
                // List<JavaRDD<Quad>> rdds = queries.stream().map(query -> JavaRddOfBindingsOps.execSparqlConstruct(initialRdd, query, null)).collect(Collectors.toList());
                // JavaRDD<Quad> rdd = javaSparkContext.union(rdds.toArray(new JavaRDD[0]));
                RdfSource rdfSource = RdfSources.ofQuads(rdd);
                CmdSansaMapImpl.writeOutRdfSources(rdfSource, rddRdfWriterFactory);
                break;
            }
            default: {
                throw new UnsupportedOperationException("Query type " + queryType + " not yet supported");
                /*
                List<JavaRDD<Binding>> rdds = queries.stream().map(query -> JavaRddOfBindingsOps.execSparqlSelect(initialRdd, query, null)).collect(Collectors.toList());
                JavaRDD<Quad> rdd = javaSparkContext.union(rdds.toArray(new JavaRDD[0]));
                RdfSource rdfSource = RdfSources.ofQuads(rdd);

                break;
                 */
            }

        }

        return 0; // exit code
    }
}
