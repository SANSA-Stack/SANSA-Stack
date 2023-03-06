package net.sansa_stack.spark.cli.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.aksw.commons.lambda.serializable.SerializableSupplier;
import org.aksw.jena_sparql_api.rx.RDFLanguagesEx;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.aksw.jenax.arq.picocli.CmdMixinArq;
import org.aksw.jenax.arq.util.exec.ExecutionContextUtils;
import org.aksw.jenax.stmt.core.SparqlStmtMgr;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.Context;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;
import net.sansa_stack.query.spark.rdd.op.JavaRddOfBindingsOps;
import net.sansa_stack.spark.cli.cmd.CmdSansaNgsQuery;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactoryImpl;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfDatasetsOps;

/**
 * Called from the Java class [[CmdSansaNgsQuery]]
 */
public class CmdSansaNgsQueryImpl {
  private static final Logger logger = LoggerFactory.getLogger(CmdSansaNgsQueryImpl.class);
    // JenaSystem.init()


  public static Integer run(CmdSansaNgsQuery cmd) {

    List<Lang> resultSetFormats = RDFLanguagesEx.getResultSetFormats();
    Lang outLang;

    Query query = SparqlStmtMgr.loadQuery(cmd.queryFile);
    logger.info("Loaded query " + query);

    if (cmd.outputConfig.outFormat != null) {
        outLang = RDFLanguagesEx.findLang(cmd.outputConfig.outFormat, resultSetFormats);
    } else {
        // TODO Default based on the query type
        outLang = ResultSetLang.RS_JSON;
    }

    if (outLang == null) {
        throw new IllegalArgumentException("No result set format found for " + cmd.outputConfig.outFormat);
    }

    logger.info("Detected registered result set format: " + outLang);

    // cmd.outputConfig.outFormat
    // RddRdfWriterFactory rddRdfWriterFactory = CmdUtils.configureWriter(cmd.outputConfig);

    SparkSession sparkSession = CmdUtils.newDefaultSparkSessionBuilder()
            .appName("Sansa Ngs Query (" + cmd.inputFiles + ")")
            .config("spark.sql.crossJoin.enabled", true)
            .getOrCreate();

    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());


    StopWatch stopwatch = StopWatch.createStarted();

    RdfSourceFactory rdfSourceFactory = RdfSourceFactoryImpl.from(sparkSession);
    List<JavaRDD<DatasetOneNg>> sources = new ArrayList<>();
    for (String input : cmd.inputFiles) {
      RdfSource rdfSource = rdfSourceFactory.get(input);
      // Lang lang = rdfSource.getLang();
      sources.add(rdfSource.asDatasets().toJavaRDD());
    }

    JavaRDD<DatasetOneNg> rdd = javaSparkContext.union(sources.toArray(new JavaRDD[0]));

    rdd = cmd.makeDistinct
            ? JavaRddOfDatasetsOps.groupNamedGraphsByGraphIri(rdd, cmd.makeDistinct, false, -1)
            : rdd;


    // CmdMixinArq is serializable
    CmdMixinArq arqConfig = cmd.arqConfig;
    CmdMixinArq.configureGlobal(arqConfig);
    // TODO Jena ScriptFunction searches for JavaScript LibFile only searched in the global context
    CmdMixinArq.configureCxt(ARQ.getContext(), arqConfig);
    Supplier<ExecutionContext> execCxtSupplier = CmdUtils.createExecCxtSupplier(arqConfig);

    JavaResultSetSpark resultSetSpark = JavaRddOfBindingsOps.execSparqlSelect(rdd, query, execCxtSupplier);

    ResultSetMgr.write(System.out, ResultSet.adapt(resultSetSpark.collectToTable().toRowSet()), outLang);

    logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

    return 0; // exit code
  }
}
