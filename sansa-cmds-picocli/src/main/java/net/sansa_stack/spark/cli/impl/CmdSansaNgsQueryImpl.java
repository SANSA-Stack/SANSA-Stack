package net.sansa_stack.spark.cli.impl;

import com.google.common.collect.Iterables;
import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;
import net.sansa_stack.spark.util.LifeCycle;
import net.sansa_stack.query.spark.rdd.op.JavaRddOfBindingsOps;
import net.sansa_stack.spark.cli.cmd.CmdSansaNgsQuery;
import net.sansa_stack.spark.cli.util.RdfOutputConfig;
import net.sansa_stack.spark.cli.util.SansaCmdUtils;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactories;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import net.sansa_stack.spark.io.rdf.output.RddRowSetWriterFactory;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfDatasetsOps;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOps;
import org.aksw.jena_sparql_api.rx.script.SparqlScriptProcessor;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.aksw.jenax.arq.picocli.CmdMixinArq;
import org.aksw.jenax.stmt.core.SparqlStmt;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

interface QueryProcessor<T> {
  void exec(JavaRDD<DatasetOneNg> inputRdd, LifeCycle<ExecutionContext> execCxtLifeCycle);
}


class QueryProcessorFactory {

  public static QueryProcessor create(Query query, RdfOutputConfig conf) {
    QueryProcessor result;
    if (query.isSelectType()) {
      RddRowSetWriterFactory writerFactory = SansaCmdUtils.configureRowSetWriter(conf);
      result = (inputRdd, execCxtSupplier) -> {
        JavaResultSetSpark rowSet = JavaRddOfBindingsOps.execSparqlSelect(inputRdd, query, execCxtSupplier);
        writerFactory.forRowSet(rowSet).runUnchecked();
      };
    } else if (query.isConstructType()) {
      RddRdfWriterFactory writerFactory = SansaCmdUtils.configureRdfWriter(conf);
      if (query.isConstructQuad()) {
        result = (inputRdd, execCxtSupplier) -> {
          JavaRDD<DatasetOneNg> rdd = JavaRddOfBindingsOps.execSparqlConstructDatasets(inputRdd, query, execCxtSupplier);
          writerFactory.forDataset(rdd).runUnchecked();
        };
      } else {
        result = (inputRdd, execCxtSupplier) -> {
          JavaRDD<Triple> rdd = JavaRddOfBindingsOps.execSparqlConstructTriples(inputRdd, query, execCxtSupplier);
          writerFactory.forTriple(rdd).runUnchecked();
        };
      }
    } else {
      throw new UnsupportedOperationException("Unsupported query type");
    }
    return result;
  }
}


/**
 * Called from the Java class [[CmdSansaNgsQuery]]
 */
public class CmdSansaNgsQueryImpl {
  private static final Logger logger = LoggerFactory.getLogger(CmdSansaNgsQueryImpl.class);
    // JenaSystem.init()


  public static Integer run(CmdSansaNgsQuery cmd) {

    // List<Lang> resultSetFormats = RDFLanguagesEx.getResultSetFormats();
    // Lang outLang;

    SparqlScriptProcessor processor = SparqlScriptProcessor.createPlain(null, null);
    processor.process(cmd.queryFile);
    SparqlStmt stmt = Iterables.getOnlyElement(processor.getPlainSparqlStmts());
    Query query = stmt.getQuery();
    logger.info("Loaded query " + query);

    QueryProcessor queryProcessor = QueryProcessorFactory.create(query, cmd.outputConfig);


    // RddRdfWriterFactory rddRdfWriterFactory = SansaCmdUtils.configureRdfWriter(cmd.outputConfig);

/*
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
*/
    // cmd.outputConfig.outFormat
    // RddRdfWriterFactory rddRdfWriterFactory = CmdUtils.configureWriter(cmd.outputConfig);

    SparkSession sparkSession = SansaCmdUtils.newDefaultSparkSessionBuilder()
            .appName("Sansa Ngs Query (" + cmd.inputFiles + ")")
            .config("spark.sql.crossJoin.enabled", true)
            .getOrCreate();

    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());


    StopWatch stopwatch = StopWatch.createStarted();

    RdfSourceFactory rdfSourceFactory = RdfSourceFactories.of(sparkSession);
    List<JavaRDD<DatasetOneNg>> sources = new ArrayList<>();
    for (String input : cmd.inputFiles) {
      RdfSource rdfSource = rdfSourceFactory.get(input);
      // Lang lang = rdfSource.getLang();
      sources.add(rdfSource.asDatasets().toJavaRDD());
    }

    JavaRDD<DatasetOneNg> rdd = JavaRddOps.unionIfNeeded(javaSparkContext, sources);

    rdd = cmd.makeDistinct
            ? JavaRddOfDatasetsOps.groupNamedGraphsByGraphIri(rdd, cmd.makeDistinct, false, -1)
            : rdd;


    // CmdMixinArq is serializable
    CmdMixinArq arqConfig = cmd.arqConfig;
    CmdMixinArq.configureGlobal(arqConfig);
    // TODO Jena ScriptFunction searches for JavaScript LibFile only searched in the global context
    CmdMixinArq.configureCxt(ARQ.getContext(), arqConfig);
    LifeCycle<ExecutionContext> execCxtSupplier = SansaCmdUtils.createExecCxtLifeCycle(arqConfig);

    queryProcessor.exec(rdd, execCxtSupplier);
   //  JavaResultSetSpark resultSetSpark = JavaRddOfBindingsOps.execSparqlSelect(rdd, query, execCxtSupplier);

    // Path outPath = new Path("/tmp/test.csv");
    // RddRowSetWriter.write(resultSetSpark, outPath, ResultSetLang.RS_TSV);

    // ResultSetMgr.write(System.out, ResultSet.adapt(resultSetSpark.collectToTable().toRowSet()), outLang);

    logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

    return 0; // exit code
  }
}
