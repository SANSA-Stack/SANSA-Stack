package net.sansa_stack.hadoop.format.jena.ntriples;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.core.pattern.CustomPatternJava;
import net.sansa_stack.hadoop.format.jena.base.RecordReaderGenericRdfNonAccumulatingBase;
import org.aksw.jenax.sparql.query.rx.RDFDataMgrRx;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;

import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class RecordReaderRdfNTriples
        extends RecordReaderGenericRdfNonAccumulatingBase<Triple>
{

    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.ntriples.triple.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.ntriples.triple.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.ntriples.triple.record.probecount";

    /**
     * Match the first character after a newline
     */
    protected static final CustomPattern nTriplesRecordStartPattern = CustomPatternJava.compile(
            "(?<=\\n).", Pattern.DOTALL);

    public RecordReaderRdfNTriples() {
        super(
                RECORD_MINLENGTH_KEY,
                RECORD_MAXLENGTH_KEY,
                RECORD_PROBECOUNT_KEY,
                null, // ntriples does not support prefixes
                nTriplesRecordStartPattern,
                Lang.NTRIPLES);
    }

    @Override
    protected Stream<Triple> parse(InputStream in) {
        Stream<Triple> result = RDFDataMgrRx.createFlowableTriples(() -> in, lang, baseIri).blockingStream();
        return result;
    }

    protected Flowable<Triple> parse(Callable<InputStream> inputStreamSupplier) {
        Flowable<Triple> result = RDFDataMgrRx.createFlowableTriples(inputStreamSupplier, lang, baseIri);
        return result;
    }
}
