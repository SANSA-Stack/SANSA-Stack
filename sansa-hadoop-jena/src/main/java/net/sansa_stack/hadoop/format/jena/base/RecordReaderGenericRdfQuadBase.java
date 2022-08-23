package net.sansa_stack.hadoop.format.jena.base;

import org.apache.jena.sparql.core.Quad;

import java.io.InputStream;
import java.util.stream.Stream;

public class RecordReaderGenericRdfQuadBase
        extends RecordReaderGenericRdfNonAccumulatingBase<Quad>
{
    public RecordReaderGenericRdfQuadBase(RecordReaderRdfConf conf)  {
        super(conf);
    }

    @Override
    protected Stream<Quad> parse(InputStream inputStream, boolean isProbe) {
        Stream<Quad> result = setupParser(inputStream, isProbe).streamQuads();
        return result;
    }
}
