package net.sansa_stack.spark.rdd.op.rx;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

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
