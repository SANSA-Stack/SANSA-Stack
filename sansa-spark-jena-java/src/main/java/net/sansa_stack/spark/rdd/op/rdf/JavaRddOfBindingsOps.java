package net.sansa_stack.spark.rdd.op.rdf;

import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import net.sansa_stack.spark.rdd.op.rx.JavaRddRxOps;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import org.aksw.commons.rx.function.RxFunction;
import org.aksw.commons.util.stream.StreamFunction;
import org.aksw.jena_sparql_api.rx.query_flow.QueryFlowOps;
import org.aksw.jenax.arq.util.quad.QuadPatternUtils;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.optimize.TransformExtendCombine;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.modify.TemplateLib;
import org.apache.jena.sparql.syntax.Template;
import org.apache.jena.sparql.util.Context;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class JavaRddOfBindingsOps {

    /** The special ?ROWNUM variable supported by tarql */
    public static final Var ROWNUM = Var.alloc("ROWNUM");

    /**
     * Use an RDD of bindings as initial bindings for a construct query in order to yield triples.
     * This is conceptually the same approach as done by the tool 'tarql', hence the name.
     */
    public static JavaRDD<Triple> tarqlTriples(JavaRDD<Binding> rdd, Query query) {
        // On xps 17: processing times of stream vs rx on pdl data yields 3:15 vs 3:30min; so stream is faster ~ Claus
        return tarqlTriplesStream(rdd, query);
    }

    public static JavaRDD<Triple> tarqlTriplesStream(JavaRDD<Binding> rdd, Query query) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
        Broadcast<Query> queryBc = cxt.broadcast(query);
        StreamFunction<Binding, Triple> mapper = upstream -> {
            Query q = queryBc.getValue();
            Template template = q.getConstructTemplate();
            Op op = Algebra.compile(q);
            op = tarqlOptimize(op);

            return StreamFunction.<Binding>identity()
                    .andThen(JavaRddOfBindingsOps.createMapperBindings(op))
                    .andThenFlatMap(JavaRddOfBindingsOps.createMapperTriples(template)::apply)
                    .apply(upstream);
//            return upstream
//                    .compose(QueryFlowOps.createMapperBindings(op))
//                    .flatMap(QueryFlowOps.createMapperTriples(template)::apply);
        };

        rdd = enrichRddWithRowNumIfNeeded(rdd, query);
        return JavaRddOps.mapPartitions(rdd, mapper);
    }

    public static StreamFunction<Binding, Binding> createMapperBindings(Op op) {
        return upstream -> {
            DatasetGraph ds = DatasetGraphFactory.create();
            Context cxt = ARQ.getContext().copy();
            ExecutionContext execCxt = new ExecutionContext(cxt, ds.getDefaultGraph(), ds, QC.getFactory(cxt));

            return upstream.flatMap(binding -> {
                QueryIterator r = QC.execute(op, binding, execCxt);
                Iter.onClose(r, r::close);
                return Streams.stream(r).onClose(r::close);
            });
        };
    }

    public static Function<Binding, Stream<Triple>> createMapperTriples(Template template) {
        return binding -> Streams.stream(TemplateLib.calcTriples(template.getTriples(), Collections.singleton(binding).iterator()));
    }

    public static JavaRDD<Triple> tarqlTriplesRx(JavaRDD<Binding> rdd, Query query) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
        Broadcast<Query> queryBc = cxt.broadcast(query);
        RxFunction<Binding, Triple> mapper = upstream -> {
            Query q = queryBc.getValue();
            Template template = q.getConstructTemplate();
            Op op = Algebra.compile(q);
            op = tarqlOptimize(op);

            return upstream
                    .compose(QueryFlowOps.createMapperBindings(op))
                    .flatMap(QueryFlowOps.createMapperTriples(template)::apply);
        };

        rdd = enrichRddWithRowNumIfNeeded(rdd, query);
        return JavaRddRxOps.mapPartitions(rdd, mapper);
    }

    /** Apply default optimizations for algebra expressions meant for tarql
     * Combines EXTENDS */
    public static Op tarqlOptimize(Op op) {
        Op result = Transformer.transform(new TransformExtendCombine(), op);
        return result;
    }

    /**
     * Use an RDD of bindings as initial bindings for a construct query in order to yield quads.
     * This is conceptually the same approach as done by the tool 'tarql', hence the name.
     */
    public static JavaRDD<Quad> tarqlQuads(JavaRDD<Binding> rdd, Query query) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
        Broadcast<Query> queryBc = cxt.broadcast(query);
        RxFunction<Binding, Quad> mapper = upstream -> {
            Query q = queryBc.getValue();
            Template template = q.getConstructTemplate();
            Op op = Algebra.compile(q);
            op = tarqlOptimize(op);

            return upstream
                    .compose(QueryFlowOps.createMapperBindings(op))
                    .flatMap(QueryFlowOps.createMapperQuads(template)::apply);
        };

        rdd = enrichRddWithRowNumIfNeeded(rdd, query);
        return JavaRddRxOps.mapPartitions(rdd, mapper);
    }

    /** Each binding becomes its own dataset */
    public static JavaRDD<Dataset> tarqlDatasets(JavaRDD<Binding> rdd, Query query) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
        Broadcast<Query> queryBc = cxt.broadcast(query);
        RxFunction<Binding, Dataset> mapper = upstream -> {
            Query q = queryBc.getValue();
            Template template = q.getConstructTemplate();
            Op op = Algebra.compile(q);
            op = tarqlOptimize(op);

            return upstream
                    .compose(QueryFlowOps.createMapperBindings(op))
                    .flatMap(QueryFlowOps.createMapperQuads(template)::apply)
                    .reduceWith(DatasetGraphFactory::create, (dsg, quad) -> { dsg.add(quad); return dsg; })
                    .map(DatasetFactory::wrap)
                    .toFlowable();
        };

        rdd = enrichRddWithRowNumIfNeeded(rdd, query);
        return JavaRddRxOps.mapPartitions(rdd, mapper);
    }

    /** If the given query mentions a variable ?ROWNUM (upper case) then the input rdd of bindings is
     *  zipped with index */
    public static JavaRDD<Binding> enrichRddWithRowNumIfNeeded(JavaRDD<Binding> rdd, Query query) {
        Set<Var> mentionedVars = query.isConstructType()
                ? QuadPatternUtils.getVarsMentioned(query.getConstructTemplate().getQuads())
                : new HashSet<>();

        Op op = Algebra.compile(query);
        Collection<Var> patternVars = OpVars.mentionedVars(op);
        mentionedVars.addAll(patternVars);
        boolean usesRowNum = mentionedVars.contains(ROWNUM);

        JavaRDD<Binding> result = usesRowNum
                ? rdd.zipWithIndex().map(bi -> BindingFactory.binding(bi._1, ROWNUM, NodeValue.makeInteger(bi._2 + 1).asNode()))
                : rdd;

        return result;
    }
}
