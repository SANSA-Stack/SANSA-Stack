package net.sansa_stack.spark.rdd.function;

import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.apache.spark.api.java.JavaRDD;

/**
 * Interface for building chains of transformations over JavaRDDs and JavaPairRDDs.
 *
 * Example usage:
 *
 * <pre>
 * {code
 * JavaRddFunction<Resource, Resource> compositeTransform =
 *     JavaRddFunction.<Resource>identity()
 *         .toPairRdd(JavaRddOfResourcesOps::mapToNamedModels)
 *         .andThen(rdd -> JavaRddOfNamedModelsOps.groupNamedModels(rdd, true, true, 0))
 *         .toRdd(JavaRddOfNamedModelsOps::mapToResources);
 *
 * JavaRDD<Resource> newRdd = compositeTransform.apply(rdd);
 * }
 * </pre>
 *
 * @param <I>
 * @param <O>
 *
 * @author Claus Stadler 2021-10-08
 */
@FunctionalInterface
public interface JavaRddFunction<I, O>
        extends SerializableFunction<JavaRDD<I>, JavaRDD<O>> {

    default <X> JavaRddFunction<I, X> andThen(JavaRddFunction<O, X> next) {
        return rdd -> next.apply(this.apply(rdd));
    }

    default <K, V> ToJavaPairRddFunction<I, K, V> toPairRdd(ToJavaPairRddFunction<O, K, V> next) {
        return rdd -> next.apply(this.apply(rdd));
    }

    static <I> JavaRddFunction<I, I> identity() {
        return x -> x;
    }
}
