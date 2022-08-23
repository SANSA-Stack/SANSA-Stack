package net.sansa_stack.hadoop.format.jena.base;

import net.sansa_stack.hadoop.core.Accumulating;
import net.sansa_stack.hadoop.core.RecordReaderGenericBase;
import org.aksw.jenax.arq.util.irixresolver.IRIxResolverUtils;
import org.aksw.jenax.sparql.query.rx.RDFDataMgrRx;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.riot.system.ErrorHandlerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public abstract class RecordReaderGenericRdfBase<U, G, A, T>
        extends RecordReaderGenericBase<U, G, A, T>
{
    protected final String baseIriKey;
    protected final String headerBytesKey;
    protected String prefixesMaxLengthKey;

    protected String baseIri;
    protected Lang lang;

    public RecordReaderGenericRdfBase(RecordReaderRdfConf conf,
            Accumulating<U, G, A, T> accumulating) {
        super(conf,
                // FileInputFormatRdfBase.BASE_IRI_KEY,
                // FileInputFormatRdfBase.PREFIXES_KEY,
                accumulating);
        this.lang = conf.getLang();
        this.prefixesMaxLengthKey = conf.getPrefixesMaxLengthKey();
        this.baseIriKey = FileInputFormatRdfBase.BASE_IRI_KEY;
        this.headerBytesKey = FileInputFormatRdfBase.PREFIXES_KEY;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        super.initialize(inputSplit, context);

        Configuration job = context.getConfiguration();

        baseIri = job.get(baseIriKey);

        Model model = FileInputFormatRdfBase.getModel(job, headerBytesKey);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RDFDataMgr.write(baos, model, RDFFormat.TURTLE_PRETTY);
        // val prefixBytes = baos.toByteArray
        preambleBytes = baos.toByteArray();
    }

    protected AsyncParserBuilder setupParser(InputStream in, boolean isProbe) {
        AsyncParserBuilder result = AsyncParser.of(in, lang, baseIri)
                .mutateSources(parser -> parser
                        .labelToNode(RDFDataMgrRx.createLabelToNodeAsGivenOrRandom())
                        .resolver(IRIxResolverUtils.newIRIxResolverAsGiven()));

        if (isProbe) {
            result = result
                    .setChunkSize(100)
                    .mutateSources(parser -> parser.errorHandler(ErrorHandlerFactory.errorHandlerSimple()));
        }

        // Stream<Quad> result = RDFDataMgrRx.createFlowableQuads(() -> in, lang, baseIri).blockingStream();
        // System.out.println("isParallel: " + result.isParallel());
        // Flowable<Dataset> result = RDFDataMgrRx.createFlowableDatasets(inputStreamSupplier, lang, baseIri);
        return result;
    }


}
