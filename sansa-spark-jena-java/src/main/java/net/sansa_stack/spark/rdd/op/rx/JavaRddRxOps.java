package net.sansa_stack.spark.rdd.op.rx;

import com.google.common.base.Preconditions;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import net.sansa_stack.rdf.spark.util.JavaSparkContextUtils;
import org.aksw.commons.rx.util.FlowableEx;
import org.aksw.jena_sparql_api.rx.query_flow.QueryFlowOps;
import org.apache.jena.graph.Triple;
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

    /**
     * Sub-interface of {@link FlowableTransformer} that extends Serializable
     *
     * TODO Move to aksw-commons-rx?
     */
    public interface SerializableFlowableTransformer<Upstream, Downstream>
            extends FlowableTransformer<Upstream, Downstream>, Serializable {}

    /** Map operation based on a flowable transformer */
    public static <I, O> JavaRDD<O> mapPartitions(JavaRDD<I> rdd, SerializableFlowableTransformer<I, O> transformer) {
        return rdd.mapPartitions(it -> Flowable.fromIterable(() -> it)
                .compose(transformer).blockingIterable().iterator());
    }


}