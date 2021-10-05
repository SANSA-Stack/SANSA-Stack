package net.sansa_stack.query.spark.ops;

import com.google.common.base.Preconditions;
import io.reactivex.rxjava3.core.FlowableTransformer;
import net.sansa_stack.rdf.spark.util.JavaSparkContextUtils;
import org.aksw.commons.rx.util.RxUtils;
import org.aksw.jena_sparql_api.rx.query_flow.QueryFlowOps;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.syntax.Template;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.Iterator;

public class JavaRddRxOps {

    // TODO Mave to aksw-commons-rx?
    public interface SerializableFlowableTransformer<Upstream, Downstream>
            extends FlowableTransformer<Upstream, Downstream>, Serializable {}

    /** Map operation based on a flowable transformer */
    public static <I, O> JavaRDD<O> map(JavaRDD<I> rdd, SerializableFlowableTransformer<I, O> transformer) {
        return rdd.mapPartitions(it -> RxUtils.<I, Iterator<I>>fromIteratorSupplier(() -> it)
                .compose(transformer).blockingIterable().iterator());
    }

    /**
     * Use an RDD of bindings as initial bindings for a construct query in order to yield quads.
     * This is conceptually the same approach as done by the tool 'tarql', hence the name.
     */
    public static JavaRDD<Quad> tarql(JavaRDD<Binding> rdd, Query query) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
        Broadcast<Query> queryBc = cxt.broadcast(query);
        SerializableFlowableTransformer<Binding, Quad> mapper = upstream -> {
                Query q = queryBc.getValue();
                Template template = q.getConstructTemplate();
                Op op = Algebra.compile(q);

                return upstream
                            .compose(QueryFlowOps.createMapperBindings(op))
                            .flatMap(QueryFlowOps.createMapperQuads(template)::apply);
        };

        return map(rdd, mapper);
        // return map(rdd, QueryFlowOps.createMapperQuads(query)::apply);
    }
}
