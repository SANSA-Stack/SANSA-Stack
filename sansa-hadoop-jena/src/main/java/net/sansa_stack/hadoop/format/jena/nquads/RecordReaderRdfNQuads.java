package net.sansa_stack.hadoop.format.jena.nquads;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.core.pattern.CustomPatternJava;
import net.sansa_stack.hadoop.format.jena.base.RecordReaderGenericRdfNonAccumulatingBase;
import org.aksw.jenax.arq.util.irixresolver.IRIxResolverUtils;
import org.aksw.jenax.sparql.query.rx.RDFDataMgrRx;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.sparql.core.Quad;

import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class RecordReaderRdfNQuads
        extends RecordReaderGenericRdfNonAccumulatingBase<Quad>
{

    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.nquads.quad.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.nquads.quad.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.nquads.quad.record.probecount";

    /**
     * Match the first character after a newline
     */
    protected static final CustomPattern nQuadsRecordStartPattern = CustomPatternJava.compile(
            "(?<=\\n).", Pattern.DOTALL);

    public RecordReaderRdfNQuads() {
        super(
                RECORD_MINLENGTH_KEY,
                RECORD_MAXLENGTH_KEY,
                RECORD_PROBECOUNT_KEY,
                null, // ntriples does not support prefixes
                nQuadsRecordStartPattern,
                Lang.NQUADS);
    }

    @Override
    protected Stream<Quad> parse(InputStream in) {
        Stream<Quad> result = AsyncParser.of(in, lang, baseIri)
                .mutateSources(parser -> parser
                        .labelToNode(RDFDataMgrRx.createLabelToNodeAsGivenOrRandom())
                        .resolver(IRIxResolverUtils.newIRIxResolverAsGiven()))
                .setChunkSize(100)
                .streamQuads();

        // Stream<Quad> result = RDFDataMgrRx.createFlowableQuads(() -> in, lang, baseIri).blockingStream();
        return result;
    }

    protected Flowable<Quad> parse(Callable<InputStream> inputStreamSupplier) {
        Flowable<Quad> result = RDFDataMgrRx.createFlowableQuads(inputStreamSupplier, lang, baseIri);
        return result;
    }
}
