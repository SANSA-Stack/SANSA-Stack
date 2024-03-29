package net.sansa_stack.hadoop.format.jena.trig;

import java.io.InputStream;
import java.util.concurrent.Callable;

import org.aksw.jenax.sparql.query.rx.RDFDataMgrRx;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.core.pattern.CustomPatternTrigGraph;
import net.sansa_stack.hadoop.format.jena.base.RecordReaderGenericRdfQuadBase;
import net.sansa_stack.hadoop.format.jena.base.RecordReaderRdfConf;

public class RecordReaderRdfTrigQuad
    extends RecordReaderGenericRdfQuadBase
{
    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.trig.quad.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.trig.quad.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.trig.quad.record.probecount";
    public static final String PREFIXES_MAXLENGTH_KEY = "mapreduce.input.trig.quad.prefixes.maxlength";


    /* The custom pattern matches graph starts more efficiently than a standard java regex */
    protected static final CustomPattern trigFwdPattern = new CustomPatternTrigGraph();

//    protected static final CustomPattern trigFwdPatternWorkingGraphOnly = CustomPatternJava
//            .compile("@?base|@?prefix|(graph\\s*)?(<[^>]*>|_?:[^-\\s]+)\\s*\\{",
//                    Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
//
//    
//    protected static final CustomPattern trigFwdPatternTmp = CustomPatternJava.compile(
//            String.join("|",
//                    "@?base",
//                    "@?prefix",
//                    "(graph\\s*)?(<[^>]*>|\\w*:[^{\\s]+)\\s*\\{",
//                    "<[^>]*>",
//                    "\\[",
//                    "\\w*:"),
//            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE); // | Pattern.MULTILINE);
//            // Pattern.compile("@?base|@?prefix|(graph\\s*)?(<[^>]*>|_?:[^-\\s]+)\\s*\\{", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    public RecordReaderRdfTrigQuad() {
        super(new RecordReaderRdfConf(
                RECORD_MINLENGTH_KEY,
                RECORD_MAXLENGTH_KEY,
                RECORD_PROBECOUNT_KEY,
                trigFwdPattern,
                PREFIXES_MAXLENGTH_KEY,
                Lang.TRIG));
    }

    // @Override
    protected Flowable<Quad> parse(Callable<InputStream> inputStreamSupplier) {
        Flowable<Quad> result = RDFDataMgrRx.createFlowableQuads(inputStreamSupplier, lang, baseIri);
        return result;
    }
}
