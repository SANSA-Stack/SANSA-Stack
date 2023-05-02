package net.sansa_stack.hadoop.format.jena.base;

import org.apache.jena.query.QueryExecException;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.ExprEvalException;

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

        result = result.map(x -> {
            if (x.isDefaultGraph()) {
                // Raise an exception without the overhead of gathering the stack trace
                throw new ExprEvalException("Quad in default graph not permitted");
            }
           return x;
        });

        return result;
    }
}
