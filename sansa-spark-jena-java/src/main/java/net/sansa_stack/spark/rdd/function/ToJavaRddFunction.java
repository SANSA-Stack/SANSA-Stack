package net.sansa_stack.spark.rdd.function;

import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 * Interface for building chains of transformations over JavaRDDs and JavaPairRDDs.
 * See {@link JavaRddFunction} for more information.
 *
 * @author Claus Stadler 2021-10-08
 */
 @FunctionalInterface
public interface ToJavaRddFunction<K, V, O>
        extends SerializableFunction<JavaPairRDD<K, V>, JavaRDD<O>> {

    default <X> ToJavaRddFunction<K, V, X> andThen(JavaRddFunction<O, X> next) {
        return rdd -> next.apply(this.apply(rdd));
    }

    default <KX, VX> JavaPairRddFunction<K, V, KX, VX> toPairRdd(ToJavaPairRddFunction<O, KX, VX> next) {
        return rdd -> next.apply(this.apply(rdd));
    }
}
