package net.sansa_stack.hadoop.parser.csv;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.generic.Accumulating;
import net.sansa_stack.hadoop.generic.RecordReaderGenericBase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 * A generic parser implementation for CSV with the offset-seeking condition that
 * CSV rows must all have the same length.
 *
 */
public class RecordReaderCsv
    extends RecordReaderGenericBase<List<String>, List<String>, List<String>, List<String>>
{
    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.csv.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.csv.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.csv.record.probecount";

    // Search for open bracket or comma
    protected static final Pattern csvFwdPattern = Pattern.compile("\n", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    public RecordReaderCsv() {
        this(
                RECORD_MINLENGTH_KEY,
                RECORD_MAXLENGTH_KEY,
                RECORD_PROBECOUNT_KEY,
                csvFwdPattern);
    }

    public RecordReaderCsv(
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            Pattern recordSearchPattern) {
        super(minRecordLengthKey, maxRecordLengthKey, probeRecordCountKey, recordSearchPattern, Accumulating.identity());
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        super.initialize(inputSplit, context);
        // postambleBytes = new byte[] {']'};
    }

    /**
     * Skip the first character if it is a comma
     *
     * @param base The base input stream
     * @return
     */

    @Override
    protected InputStream effectiveInputStream(InputStream base) {
        PushbackInputStream result = new PushbackInputStream(base);
        char FIELD_SEPARATOR='\n';
        try {
            int c = result.read();
            if (c != -1 && c != FIELD_SEPARATOR) {
                result.unread(c);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return result;
    }


    protected CSVParser newCsvParser(Reader reader) throws IOException {
        return new CSVParser(reader, CSVFormat.EXCEL);
    }

    /** State class used for Flowable.generate */
    private static class State {
        public Reader reader;
        public CSVParser csvParser = null;
        public Iterator<CSVRecord> iterator;
        public long seenRowLength = -1;

        State(Reader reader) { this.reader = reader; }
    }

    @Override
    protected Flowable<List<String>> parse(Callable<InputStream> inputStreamSupplier) {

        return Flowable.generate( // <JsonElement, Map.Entry<JsonReader, Boolean>>
                () -> new State(new InputStreamReader(inputStreamSupplier.call())),
                (s, e) -> {
                    try {
                        Iterator<CSVRecord> it = s.iterator;
                        if (it == null) {
                            s.csvParser = newCsvParser(s.reader);
                            it = s.iterator = s.csvParser.iterator();
                        }

                        if (!it.hasNext()) {
                            e.onComplete();
                        } else {
                            CSVRecord csvRecord = it.next();
                            List<String> row = csvRecord.toList();

                            long rowLength = row.size();
                            long priorRowLength = s.seenRowLength;
                            if (priorRowLength == -1) {
                                s.seenRowLength = rowLength;
                            } else if (rowLength != priorRowLength) {
                                throw new IllegalStateException(String.format("Current row length (%d) does not match prior one (%d)", rowLength, priorRowLength));
                            }
                            e.onNext(row);
                        }

                    } catch (Exception x) {
                        e.onError(x);
                    }
                },
                s -> s.csvParser.close());
    }
}
