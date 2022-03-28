package net.sansa_stack.spark.rdd.function;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Consumer interface for JavaRDDs.
 * Typically useful for invoking terminal operations on RDDs, such writing them out.
 *
 * @param <T>
 */
@FunctionalInterface
public interface JavaRddConsumer<T> extends Consumer<JavaRDD<T>>, Serializable {}
