package net.sansa_stack.hadoop.format.jena.base;

import net.sansa_stack.hadoop.core.Accumulating;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import org.apache.jena.riot.Lang;

import java.util.regex.Pattern;

public abstract class RecordReaderGenericRdfAccumulatingBase<U, G, A, T>
    extends RecordReaderGenericRdfBase<U, G, A, T>
{

    public RecordReaderGenericRdfAccumulatingBase(
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            String prefixesMaxLengthKey,
            CustomPattern recordSearchPattern,
            Lang lang,
            Accumulating<U, G, A, T> accumulating) {
        super(minRecordLengthKey,
                maxRecordLengthKey,
                probeRecordCountKey,
                prefixesMaxLengthKey,
                recordSearchPattern,
                lang,
                // FileInputFormatRdfBase.BASE_IRI_KEY,
                // FileInputFormatRdfBase.PREFIXES_KEY,
                accumulating);
    }
}
