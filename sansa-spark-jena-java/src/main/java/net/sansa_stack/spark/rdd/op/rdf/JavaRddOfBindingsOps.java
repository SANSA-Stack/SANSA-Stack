package net.sansa_stack.spark.rdd.op.rdf;

import com.google.common.base.Preconditions;
import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.aksw.commons.util.function.TriConsumer;
import org.aksw.commons.util.stream.StreamFunction;
import org.aksw.jenax.arq.util.update.UpdateUtils;
import org.aksw.jenax.stmt.core.SparqlStmt;
import org.aksw.jenax.stmt.core.SparqlStmtQuery;
import org.aksw.jenax.stmt.util.SparqlStmtUtils;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.optimize.Optimize;
import org.apache.jena.sparql.algebra.optimize.TransformExtendCombine;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.modify.TemplateLib;
import org.apache.jena.sparql.syntax.Template;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.jena.system.Txn;
import org.apache.spark.api.java.JavaRDD;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @implNote The tarql methods may be subject to reimplementation against the Binding-Engine by means of making use
 *  of the SERVICE &lt;rml.source&gt; operators. This should lead to a more unified infrastructure. Currently these
 *  methods lack support for passing custom execution contexts.
 */
public class JavaRddOfBindingsOps {

    /** The special ?ROWNUM variable supported by tarql */
    public static final Var ROWNUM = Var.alloc("ROWNUM");

    /**
     * Compile a construct query into a function that can efficiently produce triples/quads
     * from a given binding.
     * The query is internally stored in algebra form to allow for fast execution.
     */
    public static <T> BiFunction<Binding, ExecutionContext, Stream<T>> compileNodeTupleMapper(
            Query query,
            Function<Template, Function<Binding, Stream<T>>> templateMapperFactory) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

        Template template = query.getConstructTemplate();
        Op op = Algebra.compile(query);

        // The optimizer needs to run in order to properly "tag" property functions
        Op oop = Optimize.optimize(op, ARQ.getContext());
        Op finalOp = tarqlOptimize(oop);
        Function<Binding, Stream<T>> templateMapper = templateMapperFactory.apply(template);

        return (binding, execCxt) -> {
            // System.out.println("Executing binding " + binding);
            // System.out.println("Op: " + finalOp);
            // System.out.println("Context/Executor: " + execCxt.getExecutor());
            QueryIterator r = QC.execute(finalOp, binding, execCxt);
            Stream<Binding> base = Iter.asStream(r);
            //List<Binding> list = base.collect(Collectors.toList());
            // System.out.println("Evaluated to: " + list);
            // base = list.stream();
            return base.flatMap(templateMapper);
        };
    }

    public static <I, O> Function<I, O> bindToExecCxt(ExecutionContext execCxt, BiFunction<I, ExecutionContext, O> fn) {
        return bindSecondArgument(execCxt, fn);
    }

    /** Create a Function from a BiFunction by binding the second argument to a given value */
    public static <A2, I, O> Function<I, O> bindSecondArgument(A2 arg2, BiFunction<I, A2, O> fn) {
        return in -> fn.apply(in, arg2);
    }

    /*
    public static <I, O> Function<I, O> bindToEmptyDataset(BiFunction<I, ExecutionContext, O> fn) {
        return bindToDataset(fn, DatasetGraphFactory.empty());
    }

    /* * Bind a node tuple mapper to a fixed execution context. * /
    public static <I, O> Function<I, O> bindToDataset(BiFunction<I, ExecutionContext, O> fn, DatasetGraph ds) {
        Context cxt = ARQ.getContext().copy();
        OpExecutorFactory opExecutorFactory = QC.getFactory(cxt);
        // System.out.println("OpExecutorFactory: " + opExecutorFactory);
        ExecutionContext execCxt = new ExecutionContext(cxt, ds.getDefaultGraph(), ds, opExecutorFactory);
        return in -> fn.apply(in, execCxt);
    }
    */

    public static Function<Binding, Stream<Triple>> templateMapperTriples(Template template) {
        List<Triple> triples = template.getTriples();
        return binding -> Iter.asStream(TemplateLib.calcTriples(triples, Collections.singleton(binding).iterator()));
    }

    public static Function<Binding, Stream<Quad>> templateMapperQuads(Template template) {
        List<Quad> quads = template.getQuads();
        // System.out.println("Template quads: " + quads.size() + quads);
        return binding -> Iter.asStream(TemplateLib.calcQuads(quads, Collections.singleton(binding).iterator()));
    }

    public static Function<Binding, Stream<Quad>> compileTarqlMapper(List<SparqlStmt> stmts, boolean constructMode) {
        // If all queries are pattern-free construct queries we can use the fastest processing path
        // If a query uses patterns then we need to set up a dataset against which those patterns can be executed

        Function<Binding, DatasetGraph> base = compileTarqlMapperGeneral(stmts, constructMode);
        return binding -> Iter.asStream(base.apply(binding).find());
    }

    public static Function<Binding, DatasetGraph> compileTarqlMapperGeneral(Collection<SparqlStmt> stmts, boolean accumulationMode) {
        List<TriConsumer<Binding, ExecutionContext, DatasetGraph>> actions = stmts.stream().map(stmt -> {
            TriConsumer<Binding, ExecutionContext, DatasetGraph> r;
            if (stmt.isQuery()) {
                Query query = stmt.getQuery();
                if (query.isConstructType()) {
                    BiFunction<Binding, ExecutionContext, Stream<Quad>> ntm = compileNodeTupleMapper(stmt.getQuery(), JavaRddOfBindingsOps::templateMapperQuads);
                    // TODO We are updating the dataset while our query is running; we may have to store the quads in a temp collection first
                    r = (b, execCxt, outDs) -> ntm.apply(b, execCxt).forEach(outDs::add);
//                            .forEach(item -> {
//                                System.out.println("Mapped " + b + " to " + item);
//                                outDs.add(item);
//                            });
                } else  if (query.isSelectType()) {
                    // TODO Print out result set
                    throw new UnsupportedOperationException();
//					r = (b, execCxt, outDs) -> {
//						QueryExecDataset.newBuilder().dataset(execCxt.getDataset())
//						.substitution(b).s
//						System.err.println(ResultSetFormatter.asText(qe.execSelect()));

                } else if (query.isAskType()) {
                    // TODO Print out ask result
                    throw new UnsupportedOperationException();
                    // System.err.println(qe.execAsk());
                } else {
                    throw new IllegalStateException("Unknown query type: " + query);
                }
            } else {
                r = (b, execCxt, outDs) -> UpdateExec
                        .dataset(execCxt.getDataset())
                        .substitution(b)
                        .update(stmt.getUpdateRequest())
                        .execute();
            }
            return r;
        })
        .collect(Collectors.toList());

        Function<Binding, DatasetGraph> result;
        Context context = ARQ.getContext().copy() ;
        OpExecutorFactory opExecutorFactory = QC.getFactory(context);
        context.set(ARQConstants.sysCurrentTime, NodeFactoryExtra.nowAsDateTime()) ;

        result = binding -> {
            DatasetGraph r = DatasetGraphFactory.createGeneral();
            DatasetGraph inputDs = DatasetGraphFactory.createGeneral();
            ExecutionContext execCxt = new ExecutionContext(context, inputDs.getDefaultGraph(), inputDs, opExecutorFactory) ;
            Txn.executeWrite(r, () -> {
                Txn.executeWrite(inputDs, () -> {
                    for (TriConsumer<Binding, ExecutionContext, DatasetGraph> action : actions) {
                        action.accept(binding, execCxt, r);
                        if (accumulationMode) {
                            inputDs.addAll(r);
                        }
                    }
                });
            });
            // System.err.println("Construct mode; created " + Iter.count(r.find()) + " quads");
            // System.err.println("Binding " + binding + " - actions: " + actions.size());
            return accumulationMode ? inputDs : r;
        };
        return result;
    }

    public static boolean mayProduceQuads(Collection<SparqlStmt> stmts) {
        return stmts.stream().anyMatch(JavaRddOfBindingsOps::mayProduceQuads);
    }

    public static boolean mayProduceQuads(SparqlStmt stmt) {
        boolean result;
        if (!stmt.isParsed()) {
            result = true;
        } else if (stmt.isQuery()) {
            Query query = stmt.getQuery();
            result = !(query.isConstructType() && query.isConstructQuad());
        } else {
            result = true; // Updates may affect quads - we don't know
        }
        return result;
    }

    /** Method for the typical case of mapping an RDD of bindings via a construct query to an RDD of datasets. */
    public static JavaRDD<Dataset> tarqlDatasets(JavaRDD<Binding> rdd, Query query) {
        return tarqlDatasets(rdd, Collections.singleton(new SparqlStmtQuery(query)), false, dsg -> Stream.of(DatasetFactory.wrap(dsg)));
    }

    /**
     * Turns each row into a dataset based on SPARQL update statements.
     * Construct queries and select queries are print out to STDERR.
     * Use {@link UpdateUtils.constructToInsert} to convert construct queries.
     */
    public static <T> JavaRDD<T> tarqlDatasets(JavaRDD<Binding> rdd, Collection<SparqlStmt> stmts, boolean accumulationMode, SerializableFunction<DatasetGraph, Stream<T>> finisher) {
        boolean usesRowNum = mentionesRowNum(stmts);
        rdd = usesRowNum ? enrichRddWithRowNum(rdd) : rdd;
        return JavaRddOps.mapPartitions(rdd, upstream -> {
            Function<Binding, DatasetGraph> mapper = compileTarqlMapperGeneral(stmts, accumulationMode);
            return upstream.map(mapper).flatMap(dg -> finisher.apply(dg));
        });
    }

    public static JavaRDD<Triple> tarqlTriples(JavaRDD<Binding> rdd, Collection<SparqlStmt> stmts, boolean accumulationMode, Supplier<ExecutionContext> execCxtSupplier) {
        JavaRDD<Triple> result;

        // If we are in constructMode and there are no update statements then use the fast track
        // Also, if there is
        // TODO We can also use fast track if the queries are pattern free so that they cannot refer to the
        // output of a prior query
        boolean allQueries = stmts.stream().allMatch(SparqlStmt::isQuery);
        boolean canUseFastTrack = allQueries && (!accumulationMode || stmts.size() < 2);

        boolean usesRowNum = mentionesRowNum(stmts);
        rdd = usesRowNum ? enrichRddWithRowNum(rdd) : rdd;

        if (canUseFastTrack) {
            result = JavaRddOps.mapPartitions(rdd, bindings -> {
                // The SparqlStmt-to-Query conversion has to be done here because the latter is not serializable
                List<Query> queries = stmts.stream().map(SparqlStmt::getQuery).collect(Collectors.toList());
                StreamFunction<Binding, Triple> mapper = tripleMapper(queries, execCxtSupplier);
                return mapper.apply(bindings);
            });
        } else {
            result = tarqlDatasets(rdd, stmts, accumulationMode, dg -> Iter.asStream(dg.find()).map(Quad::asTriple));
        }
        return result;
    }

    public static JavaRDD<Quad> tarqlQuads(JavaRDD<Binding> rdd, Query query, Supplier<ExecutionContext> execCxtSupplier) {
        return tarqlQuads(rdd, Collections.singleton(new SparqlStmtQuery(query)), false, execCxtSupplier);
    }

    public static JavaRDD<Quad> tarqlQuads(JavaRDD<Binding> rdd, Collection<SparqlStmt> stmts, boolean accumulationMode, Supplier<ExecutionContext> execCxtSupplier) {
        JavaRDD<Quad> result;

        // If we are in constructMode and there are no update statements then use the fast track
        // Also, if there is
        // TODO We can also use fast track if the queries are pattern free so that they cannot refer to the
        // output of a prior query
        boolean allQueries = stmts.stream().allMatch(SparqlStmt::isQuery);
        boolean canUseFastTrack = allQueries && (!accumulationMode || stmts.size() < 2);

        boolean usesRowNum = mentionesRowNum(stmts);
        rdd = usesRowNum ? enrichRddWithRowNum(rdd) : rdd;

        if (canUseFastTrack) {
            result = JavaRddOps.mapPartitions(rdd, bindings -> {
                // The SparqlStmt-to-Query conversion has to be done here because the latter is not serializable
                List<Query> queries = stmts.stream().map(SparqlStmt::getQuery).collect(Collectors.toList());
                StreamFunction<Binding, Quad> mapper = quadMapper(queries, execCxtSupplier);
                return mapper.apply(bindings);
            });
        } else {
            // result = JavaRddOfDatasetsOps.flatMapToQuads(tarqlDatasets(rdd, stmts, constructMode));
            result = tarqlDatasets(rdd, stmts, accumulationMode, dg -> Iter.asStream(dg.find()));
        }
        return result;
    }

    // LEGACY CODE BELOW - needs cleanup!

    public static StreamFunction<Binding, Triple> tripleMapper(Collection<Query> queries, Supplier<ExecutionContext> execCxtSupplier) {
        ExecutionContext execCxt = execCxtSupplier.get();
        List<Function<Binding, Stream<Triple>>> mappers = queries.stream()
                .map(q -> bindToExecCxt(execCxt, compileNodeTupleMapper(q, JavaRddOfBindingsOps::templateMapperTriples)))
                .collect(Collectors.toList());

        // For every input binding apply all mappers
        return upstream -> upstream.flatMap(binding ->
            mappers.stream().flatMap(mapper -> mapper.apply(binding)));
    }

    public static StreamFunction<Binding, Quad> quadMapper(Collection<Query> queries, Supplier<ExecutionContext> execCxtSupplier) {
        ExecutionContext execCxt = execCxtSupplier.get();
        List<Function<Binding, Stream<Quad>>> mappers = queries.stream()
                .map(q -> bindToExecCxt(execCxt, compileNodeTupleMapper(q, JavaRddOfBindingsOps::templateMapperQuads)))
                .collect(Collectors.toList());

        // For every input binding apply all mappers
        return upstream -> upstream.flatMap(binding ->
            mappers.stream().flatMap(mapper -> {
                Stream<Quad> r = mapper.apply(binding);
                return r;
            }));
    }

    /** Apply default optimizations for algebra expressions meant for tarql
     * Combines EXTENDS */
    public static Op tarqlOptimize(Op op) {
        Op result = Transformer.transform(new TransformExtendCombine(), op);
        return result;
    }

    public static boolean mentionesRowNum(SparqlStmt sparqlStmt) {
        Set<Node> nodes = SparqlStmtUtils.mentionedNodes(sparqlStmt);
        boolean result = nodes.contains(ROWNUM);
        return result;
    }

    public static boolean mentionesRowNum(Collection<SparqlStmt> sparqlStmts) {
        boolean result = sparqlStmts.stream().anyMatch(JavaRddOfBindingsOps::mentionesRowNum);
        return result;
    }

    public static JavaRDD<Binding> enrichRddWithRowNum(JavaRDD<Binding> rdd) {
        return rdd.zipWithIndex().map(bi -> BindingFactory.binding(bi._1, ROWNUM, NodeValue.makeInteger(bi._2 + 1).asNode()));
    }
}
