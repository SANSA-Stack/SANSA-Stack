package net.sansa_stack.query.spark.api.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.aksw.commons.collections.IterableUtils;
import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.aksw.commons.lambda.serializable.SerializableSupplier;
import org.aksw.jena_sparql_api.rx.script.SparqlScriptProcessor;
import org.aksw.jena_sparql_api.sparql.ext.url.E_IriAsGiven;
import org.aksw.jena_sparql_api.sparql.ext.url.F_BNodeAsGiven;
import org.aksw.jenax.arq.util.exec.query.ExecutionContextUtils;
import org.aksw.jenax.arq.util.syntax.QueryUtils;
import org.aksw.jenax.stmt.core.SparqlStmt;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryType;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.Context;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import net.sansa_stack.query.spark.rdd.op.JavaRddOfBindingsOps;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSources;
import net.sansa_stack.spark.util.SansaSessionUtils;

public class QueryExecBuilder
    implements HasContextBuilder<QueryExecBuilder>
{
    protected SparkSession sparkSession;

    protected List<String> queryFiles = new ArrayList<>();

    protected boolean standardIri = false;
    protected boolean dagScheduling = false;

    protected List<Query> queries = new ArrayList<>();
    protected SerializableFunction<Context, ExecutionContext> execCxtFactory;
    protected ContextBuilder contextBuilder = new ContextBuilder();

    public static QueryExecBuilder newInstance() {
        return new QueryExecBuilder();
    }

    @Override
    public ContextBuilder getContextBuilder() {
        return contextBuilder;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public QueryExecBuilder setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        return this;
    }

    public Function<Context, ExecutionContext> getExecCxtFactory() {
        return execCxtFactory;
    }

    public QueryExecBuilder setExecCxtSupplier(SerializableFunction<Context, ExecutionContext> execCxtFactory) {
        this.execCxtFactory = execCxtFactory;
        return this;
    }

    public boolean isStandardIri() {
        return standardIri;
    }

    public QueryExecBuilder setStandardIri(boolean standardIri) {
        this.standardIri = standardIri;
        return this;
    }

    public boolean isDagScheduling() {
        return dagScheduling;
    }

    public QueryExecBuilder setDagScheduling(boolean dagScheduling) {
        this.dagScheduling = dagScheduling;
        return this;
    }

    public List<Query> getQueries() {
        return queries;
    }

    public QueryExecBuilder addFiles(Iterable<String> files) {
        for (String file : files) {
            addFile(file);
        }
        return this;
    }

    public QueryExecBuilder addFile(String file) {
        // TODO Add support to read query files from HDFS
        SparqlScriptProcessor processor = SparqlScriptProcessor.createPlain(null, null);
        processor.process(file);
        List<SparqlStmt> stmtContrib = processor.getPlainSparqlStmts();
        List<Query> queryContrib = stmtContrib.stream().map(SparqlStmt::getQuery).collect(Collectors.toList());

        addQueries(queryContrib);
        queryFiles.add(file);
        return this;
    }

    public QueryExecBuilder addQueries(Iterable<Query> queries) {
        for (Query query : queries) {
            addQuery(query);
        }
        return this;
    }

    public QueryExecBuilder addQuery(Query query) {
        Query q = QueryUtils.applyElementTransform(query, F_BNodeAsGiven.ExprTransformBNodeToBNodeAsGiven::transformElt);

        // Use fast IRI by default
        if (!standardIri) {
            q = QueryUtils.applyElementTransform(query, E_IriAsGiven.ExprTransformIriToIriAsGiven::transformElt);
        }

        addQueryDirect(q);
        return this;
    }

    /** Add a query without post processing. */
    public void addQueryDirect(Query query) {
        queries.add(query);
    }

    public RdfSource execToRdf() {
        SparkSession finalSparkSession = sparkSession;
        if (finalSparkSession == null) {
            finalSparkSession = SansaSessionUtils.newDefaultSparkSessionBuilder()
                .appName("Sansa Query (" + queryFiles.size() + " query sources)")
                .getOrCreate();
        }

        JavaSparkContext javaSparkContext = new JavaSparkContext(finalSparkSession.sparkContext());

        // TODO Perhaps make a Jena PR for StreamManager.copy() so we can place a
        // FileSystem hadoopFs = FileSystem.get(javaSparkContext.hadoopConfiguration());
        // StreamManager.get().addLocator(new LocatorHdfs(hadoopFs));

        Set<QueryType> queryTypes = queries.stream().map(Query::queryType).collect(Collectors.toSet());
        QueryType queryType = IterableUtils.expectOneItem(queryTypes); //, "Exactly one query type expected - got: " + queryTypes); // TODO Add error that mixing query types is not possible

        // Supplier<ExecutionContext> execCxtSupplier = SansaCmdUtils.createExecCxtSupplier(arqConfig);

//        PrefixMap usedPrefixes = PrefixMapFactory.create();
//        for (SparqlStmt stmt : stmts) {
//            // TODO optimizePrefixes should not modify in-place because it desyncs with the stmts's original string
//            SparqlStmtUtils.optimizePrefixes(stmt);
//            PrefixMapping pm = stmt.getPrefixMapping();
//            if (pm != null) {
//                usedPrefixes.putAll(pm);
//            }
//        }


        JavaRDD<Binding> initialRdd = JavaRddOfBindingsOps.unitRdd(javaSparkContext);

        SerializableFunction<Context, ExecutionContext> finalExecCxtCtor = execCxtFactory == null
                ? execCxtFactory
                : ExecutionContextUtils::createExecCxtEmptyDsg;

        ContextBuilder cxtBuilder = contextBuilder;
        SerializableSupplier<ExecutionContext> execCxtSupplier = () -> {
            Context cxt = cxtBuilder.build();
            ExecutionContext execCxt = finalExecCxtCtor.apply(cxt);
            return execCxt;
        };

        RdfSource result;
        switch (queryType) {
            case CONSTRUCT: {
                boolean useDag = dagScheduling;
                JavaRDD<Quad> rdd = JavaRddOfBindingsOps.execSparqlConstruct(initialRdd, queries, execCxtSupplier, useDag);
                // List<JavaRDD<Quad>> rdds = queries.stream().map(query -> JavaRddOfBindingsOps.execSparqlConstruct(initialRdd, query, null)).collect(Collectors.toList());
                // JavaRDD<Quad> rdd = javaSparkContext.union(rdds.toArray(new JavaRDD[0]));
                result = RdfSources.ofQuads(rdd);
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
        return result;
    }
}
