package net.sansa_stack.spark.rdd.function;

import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * Interface for building chains of transformations over JavaRDDs and JavaPairRDDs.
 * See {@link JavaRddFunction} for more information.
 *
 * @param <KI>
 * @param <VI>
 * @param <KO>
 * @param <VO>
 *
 * @author Claus Stadler 2021-10-08
 */
@FunctionalInterface
public interface JavaPairRddFunction<KI, VI, KO, VO>
        extends SerializableFunction<JavaPairRDD<KI, VI>, JavaPairRDD<KO, VO>> {

    default <KX, VX> JavaPairRddFunction<KI, VI, KX, VX> andThen(JavaPairRddFunction<KO, VO, KX, VX> next) {
        return rdd -> next.apply(this.apply(rdd));
    }

    default <X> ToJavaRddFunction<KI, VI, X> toRdd(ToJavaRddFunction<KO, VO, X> next) {
        return rdd -> next.apply(this.apply(rdd));
    }

    static <K, V> JavaPairRddFunction<K, V, K, V> identity() {
        return x -> x;
    }
}
