package net.sansa_stack.spark.rdd.op.rdf;

import org.aksw.commons.lambda.serializable.SerializableConsumer;
import org.aksw.commons.lambda.serializable.SerializableSupplier;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class LifeCycleImpl<T>
    implements LifeCycle<T>, Serializable
{
    protected Supplier<T> creator;
    protected Consumer<? super T> closer;

    public LifeCycleImpl(Supplier<T> creator, Consumer<? super T> closer) {
        this.creator = creator;
        this.closer = closer;
    }

    @Override
    public T newInstance() {
        return this.creator.get();
    }

    @Override
    public void closeInstance(T inst) {
        closer.accept(inst);
    }

    public static <T> LifeCycle<T> of(SerializableSupplier<T> creator, SerializableConsumer<? super T> closer) {
        return new LifeCycleImpl<>(creator, closer);
    }
}
