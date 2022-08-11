package net.sansa_stack.hadoop.format.jena.trig;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.core.pattern.CustomPatternJava;
import net.sansa_stack.hadoop.format.jena.base.RecordReaderGenericRdfNonAccumulatingBase;
import org.aksw.jenax.sparql.query.rx.RDFDataMgrRx;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RecordReaderRdfTrigQuad
    extends RecordReaderGenericRdfNonAccumulatingBase<Quad>
{
    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.trig.quad.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.trig.quad.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.trig.quad.record.probecount";
    public static final String PREFIXES_MAXLENGTH_KEY = "mapreduce.input.trig.quad.prefixes.maxlength";

    protected static final CustomPattern trigFwdPattern = CustomPatternJava.compile(
            String.join("|",
                    "@?base",
                    "@?prefix",
                    "(graph\\s*)?(<[^>]*>|\\w*:[^{\\s]+)\\s*\\{",
                    "<[^>]*>",
                    "\\[",
                    "\\w*:"),
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE); // | Pattern.MULTILINE);
            // Pattern.compile("@?base|@?prefix|(graph\\s*)?(<[^>]*>|_?:[^-\\s]+)\\s*\\{", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    public RecordReaderRdfTrigQuad() {
        super(
                RECORD_MINLENGTH_KEY,
                RECORD_MAXLENGTH_KEY,
                RECORD_PROBECOUNT_KEY,
                PREFIXES_MAXLENGTH_KEY,
                trigFwdPattern,
                Lang.TRIG);
    }

    @Override
    protected Stream<Quad> parse(InputStream in) {
        Stream<Quad> result = RDFDataMgrRx.createFlowableQuads(() -> in, lang, baseIri).blockingStream();
/*
        List<Quad> quads = result.collect(Collectors.toList());
        quads.forEach(q -> System.out.println(q));
        result = quads.stream();
*/

        // Flowable<Quad> result = RDFDataMgrRx.createFlowableQuads(inputStreamSupplier, lang, baseIri);
        return result;
    }

    // @Override
    protected Flowable<Quad> parse(Callable<InputStream> inputStreamSupplier) {
        Flowable<Quad> result = RDFDataMgrRx.createFlowableQuads(inputStreamSupplier, lang, baseIri);
        return result;
    }
}
