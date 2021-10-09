package net.sansa_stack.spark.rdd.op.rx;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import net.sansa_stack.spark.rdd.function.JavaRddFunction;
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

    /**
     * Wrap a {@link SerializableFlowableTransformer} as a {@link JavaRddFunction}
     */
    public static <I, O> JavaRddFunction<I, O> asJavaRddFunction(SerializableFlowableTransformer<I, O> transformer) {
        return rdd -> mapPartitions(rdd, transformer);
    }

    /** Map operation based on a flowable transformer */
    public static <I, O> JavaRDD<O> mapPartitions(JavaRDD<I> rdd, SerializableFlowableTransformer<I, O> transformer) {
        return rdd.mapPartitions(it -> Flowable.fromIterable(() -> it)
                .compose(transformer).blockingIterable().iterator());
    }


}
