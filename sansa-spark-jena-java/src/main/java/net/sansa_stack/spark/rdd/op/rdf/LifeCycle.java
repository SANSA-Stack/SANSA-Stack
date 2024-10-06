package net.sansa_stack.spark.rdd.op.rdf;

public interface LifeCycle<T> {
    T newInstance();
    void closeInstance(T inst);
}
