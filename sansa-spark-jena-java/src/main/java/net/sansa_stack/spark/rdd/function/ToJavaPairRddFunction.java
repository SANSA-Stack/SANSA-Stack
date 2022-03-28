package net.sansa_stack.spark.rdd.function;

import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 * Interface for building chains of transformations over JavaRDDs and JavaPairRDDs.
 * See {@link JavaRddFunction} for more information.
 *
 * @param <I>
 * @param <K>
 * @param <V>
 *
 * @author Claus Stadler 2021-10-08
 */
@FunctionalInterface
public interface ToJavaPairRddFunction<I, K, V>
        extends SerializableFunction<JavaRDD<I>, JavaPairRDD<K, V>> {

    default <KO, VO> ToJavaPairRddFunction<I, KO, VO> andThen(JavaPairRddFunction<K, V, KO, VO> next) {
        return rdd -> next.apply(this.apply(rdd));
    }

    default <O> JavaRddFunction<I, O> toRdd(ToJavaRddFunction<K, V, O> next) {
        return rdd -> next.apply(this.apply(rdd));
    }
}
