package net.sansa_stack.hadoop.parser.csv;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.generic.Accumulating;
import net.sansa_stack.hadoop.generic.RecordReaderGenericBase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A generic parser implementation for CSV with the offset-seeking condition that
 * CSV rows must all have the same length.
 *
 */
public class RecordReaderCsv
    extends RecordReaderGenericBase<List, List, List, List>
{
    private static final Logger logger = LoggerFactory.getLogger(RecordReaderCsv.class);

    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.csv.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.csv.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.csv.record.probecount";

    // Match a character (the dot at the end) if for the preceeding character it holds:
    // Match a newline; however only if there is no subsequent sole double quote followed by a newline, comma or eof.
    // Stop matching 'dot' if there was a single quote in the past
    // FIXME Make the lookahead amount configurable
    protected static final Pattern csvFwdPattern = Pattern.compile(
            "(?<=\n(?!((?<![^\"]\"[^\"]).){0,100000}\"(\r?\n|,|$))).",
            Pattern.DOTALL | Pattern.MULTILINE);

    // Failed regexes
    // "(?!(([^\"]|\"\")*\"(\r?\n\r?|,|$)))(?:\r?\n\r?).",
    //"(([^\"]{0,1000}(\"\")?){0,100}\"(?:\r?\n|,|$))?[^,\n]*.", // horrible backtracking; times out
    //"\n(?!.{0,50000}(?<!\")\"(\r?\n|,|$))" // somewhat works but too slow!

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
        public List<String> priorRow = null;

        State(Reader reader) { this.reader = reader; }
    }

    @Override
    protected Flowable<List> parse(Callable<InputStream> inputStreamSupplier) {

        return Flowable.generate(
                () -> new State(new InputStreamReader(inputStreamSupplier.call())),
                (s, e) -> {
//                    BufferedReader br = new BufferedReader(s.reader);
//                    List<String> lines = br.lines().limit(2).collect(Collectors.toList());
//                    System.out.println("Lines: " + lines);
//                    if (true) throw new RuntimeException("dummy exception");

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
                            if (s.seenRowLength == -1) {
                                s.seenRowLength = rowLength;
                            } else if (rowLength != s.seenRowLength) {
                                /*
                                String msg = String.format("Current row length (%d) does not match prior one (%d)", rowLength, s.seenRowLength);
                                logger.error(msg);
                                logger.error("Prior row: " + s.priorRow);
                                logger.error("Current row: " + row);
                                 */
                                String msg = String.format("Current row length (%d) does not match prior one (%d) - current: %s | prior: %s", rowLength, s.seenRowLength, row, s.priorRow);
                                throw new IllegalStateException(msg);
                            }
                            s.priorRow = row;
                            e.onNext(row);
                        }

                    } catch (Exception x) {
                        e.onError(x);
                    }
                },
                s -> s.csvParser.close());
    }
}
