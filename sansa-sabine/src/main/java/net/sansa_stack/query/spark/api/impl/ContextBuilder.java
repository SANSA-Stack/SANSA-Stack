package net.sansa_stack.query.spark.api.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.aksw.commons.lambda.serializable.SerializableSupplier;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.util.Context;

public class ContextBuilder
    implements Serializable
{
    private static final long serialVersionUID = 1L;

    protected Supplier<Context> baseContext;
    protected List<Consumer<Context>> contextMutators = new ArrayList<>();

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

    public Context build() {
        Context result = baseContext == null
                ? ARQ.getContext().copy()
                : baseContext.get();
        for (Consumer<Context> mutator : contextMutators) {
            mutator.accept(result);
        }
        return result;
    }
}
