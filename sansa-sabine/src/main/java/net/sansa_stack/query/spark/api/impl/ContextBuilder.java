package net.sansa_stack.query.spark.api.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import net.sansa_stack.spark.rdd.op.rdf.LifeCycle;
import net.sansa_stack.spark.rdd.op.rdf.LifeCycleImpl;
import org.aksw.commons.lambda.serializable.SerializableSupplier;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.util.Context;

public class ContextBuilder
    implements Serializable
{
    private static final long serialVersionUID = 1L;

    protected Supplier<Context> baseContext;
    protected List<Consumer<Context>> contextMutators = new ArrayList<>();

    protected List<Consumer<Context>> contextClosers = new ArrayList<>();

    public static ContextBuilder newBuilder() {
        return new ContextBuilder();
    }

    public ContextBuilder setBaseContext(SerializableSupplier<Context> baseContext) {
        this.baseContext = baseContext;
        return this;
    }

    public void addContextMutator(Consumer<Context> contextMutator) {
        this.contextMutators.add(contextMutator);
    }
    public void addContextCloser(Consumer<Context> contextMutator) {
        this.contextClosers.add(contextMutator);
    }

    public LifeCycle<Context> build() {
        List<Consumer<Context>> finalContextMutators = new ArrayList<>(contextMutators);
        List<Consumer<Context>> finalContextClosers = new ArrayList<>(contextClosers);

        return LifeCycleImpl.of(() -> {
            Context r = baseContext == null
                    ? ARQ.getContext().copy()
                    : baseContext.get();
            for (Consumer<Context> mutator : finalContextMutators) {
                mutator.accept(r);
            }
            return r;
        }, cxt -> {
            for (Consumer<Context> closer : finalContextClosers) {
                closer.accept(cxt);
            }
        });
    }
}
