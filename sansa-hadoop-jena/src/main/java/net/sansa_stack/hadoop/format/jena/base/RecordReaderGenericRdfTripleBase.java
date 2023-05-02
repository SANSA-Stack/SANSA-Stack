package net.sansa_stack.hadoop.format.jena.base;

import org.apache.jena.graph.Triple;

import java.io.InputStream;
import java.util.stream.Stream;

public class RecordReaderGenericRdfTripleBase
    extends RecordReaderGenericRdfNonAccumulatingBase<Triple>
{
    public RecordReaderGenericRdfTripleBase(RecordReaderRdfConf conf)  {
        super(conf);
    }

    @Override
    protected Stream<Triple> parse(InputStream inputStream, boolean isProbe) {
        Stream<Triple> result = setupParser(inputStream, isProbe).streamTriples();
        return result;
    }
}
