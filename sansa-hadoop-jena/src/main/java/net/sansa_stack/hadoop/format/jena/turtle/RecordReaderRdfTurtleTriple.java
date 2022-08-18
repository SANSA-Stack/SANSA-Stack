package net.sansa_stack.hadoop.format.jena.turtle;

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

public class RecordReaderRdfTurtleTriple
        extends RecordReaderGenericRdfNonAccumulatingBase<Triple>
{

    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.turtle.triple.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.turtle.triple.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.turtle.triple.record.probecount";
    public static final String PREFIXES_MAXLENGTH_KEY = "mapreduce.input.turtle.triple.prefixes.maxlength";

    /**
     * Syntatic constructs in Turtle can start with:
     *
     * TODO Anything missing?
     *
     * <ul>
     *   <li>base / @base</li>
     *   <li>prefix / @prefix</li>
     *   <li>@lt;foo;&gt; - an IRI</li>
     *   <li>[  ] - a blank node</li>
     *   <li>foo: - a CURIE</li>
     * </ul>
     *
     */
    protected static final CustomPattern turtleRecordStartPattern = CustomPatternJava.compile(
            String.join("|",
                    "@?base",
                    "@?prefix",
                    "<[^>]*>",
                    "\\[",
                    "\\w*:"),
            Pattern.CASE_INSENSITIVE); // | Pattern.MULTILINE);

    public RecordReaderRdfTurtleTriple() {
        super(
                RECORD_MINLENGTH_KEY,
                RECORD_MAXLENGTH_KEY,
                RECORD_PROBECOUNT_KEY,
                PREFIXES_MAXLENGTH_KEY,
                turtleRecordStartPattern,
                Lang.TURTLE);
    }

    @Override
    protected Stream<Triple> parse(InputStream in) {
        // Stream<Triple> result = RDFDataMgrRx.createFlowableTriples(() -> in, lang, baseIri).blockingStream();
        Stream<Triple> result = AsyncParser.of(in, lang, baseIri)
                .mutateSources(parser -> parser
                        .labelToNode(RDFDataMgrRx.createLabelToNodeAsGivenOrRandom())
                        .resolver(IRIxResolverUtils.newIRIxResolverAsGiven()))
                .setChunkSize(100)
                .streamTriples();
        return result;
    }

    protected Flowable<Triple> parse(Callable<InputStream> inputStreamSupplier) {
        Flowable<Triple> result = RDFDataMgrRx.createFlowableTriples(inputStreamSupplier, lang, baseIri);
        return result;

//        boolean showData = false;
//        if (showData) {
//            String str = null;
//            try {
//                str = IOUtils.toString(inputStreamSupplier.call(), StandardCharsets.UTF_8);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//            System.out.println("Provided Data: ------------------------------------------");
//            System.out.println(str);
//
//            byte[] bytes = str.getBytes();
//            inputStreamSupplier = () -> new ByteArrayInputStream(bytes);
//        }
    }
}
