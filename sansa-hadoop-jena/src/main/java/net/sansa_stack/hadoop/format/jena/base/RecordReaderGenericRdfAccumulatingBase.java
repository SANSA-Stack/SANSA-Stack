package net.sansa_stack.hadoop.format.jena.base;

import net.sansa_stack.hadoop.core.Accumulating;

public abstract class RecordReaderGenericRdfAccumulatingBase<U, G, A, T>
    extends RecordReaderGenericRdfBase<U, G, A, T>
{

    public RecordReaderGenericRdfAccumulatingBase(RecordReaderRdfConf conf,
            Accumulating<U, G, A, T> accumulating) {
        super(conf, accumulating);
    }
}
