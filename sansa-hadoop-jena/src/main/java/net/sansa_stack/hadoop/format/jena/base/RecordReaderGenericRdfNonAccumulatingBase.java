package net.sansa_stack.hadoop.format.jena.base;

import net.sansa_stack.hadoop.core.Accumulating;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import org.apache.jena.riot.Lang;

import java.util.regex.Pattern;

public abstract class RecordReaderGenericRdfNonAccumulatingBase<T>
    extends RecordReaderGenericRdfBase<T, T, T, T>
{

    public RecordReaderGenericRdfNonAccumulatingBase(
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            String prefixesMaxLengthKey,
            CustomPattern recordSearchPattern,
            Lang lang) {
        super(minRecordLengthKey,
                maxRecordLengthKey,
                probeRecordCountKey,
                prefixesMaxLengthKey,
                recordSearchPattern,
                lang,
                // FileInputFormatRdfBase.BASE_IRI_KEY,
                // FileInputFormatRdfBase.PREFIXES_KEY,
                Accumulating.identity());
    }
}
