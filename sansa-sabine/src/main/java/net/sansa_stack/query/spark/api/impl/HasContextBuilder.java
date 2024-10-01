package net.sansa_stack.query.spark.api.impl;

import org.aksw.commons.lambda.serializable.SerializableConsumer;
import org.aksw.commons.lambda.serializable.SerializableSupplier;
import org.aksw.commons.util.obj.HasSelf;
import org.apache.jena.sparql.util.Context;

public interface HasContextBuilder<X extends HasContextBuilder<X>>
    extends HasSelf<X>
{
    ContextBuilder getContextBuilder();

    default X addContextMutator(SerializableConsumer<Context> cxtMutator) {
        getContextBuilder().addContextMutator(cxtMutator);
        return self();
    }

    default X setBaseContext(SerializableSupplier<Context> baseContext) {
        getContextBuilder().setBaseContext(baseContext);
        return self();
    }
}
