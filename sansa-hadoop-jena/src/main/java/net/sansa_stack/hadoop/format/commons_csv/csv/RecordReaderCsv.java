package net.sansa_stack.hadoop.format.commons_csv.csv;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.core.Accumulating;
import net.sansa_stack.hadoop.core.RecordReaderGenericBase;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.core.pattern.CustomPatternJava;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    extends RecordReaderGenericBase<List, List, List, List>
{
    private static final Logger logger = LoggerFactory.getLogger(RecordReaderCsv.class);

    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.csv.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.csv.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.csv.record.probecount";

    /** Key for the serialized bytes of a {@link CSVFormat} instance */
    public static final String CSV_FORMAT_RAW_KEY = "mapreduce.input.csv.format.raw";

    /**
     * The maximum length of a CSV cell containing new lines
     *
     */
    public static final String CELL_MAXLENGTH_KEY = "mapreduce.input.csv.cell.maxlength";
    public static final long CELL_MAXLENGTH_DEFAULT_VALUE = 300000;

    /* The csv format used by this instance. Initialized during initialize() */
    protected CSVFormat requestedCsvFormat;
    protected CSVFormat effectiveCsvFormat;

    /**
     * Create a regex for matching csv record starts.
     * Matches the character following a newline character, whereas that newline is not within a csv cell
     * w.r.t. a certain amount of lookahead.
     *
     * The regex does the following:
     * Match a character (the dot at the end) if for the preceeding character it holds:
     * Match a newline; however only if there is no subsequent sole double quote followed by a newline, comma or eof.
     * Stop matching 'dot' if there was a single quote in the past
     *
     * @param n The maximum number of lookahead bytes to check for whether a newline character
     *          might be within a csv cell
     * @return The corresponding regex pattern
     */
    public static CustomPattern createStartOfCsvRecordPattern(long n) {
        return CustomPatternJava.compile(
                "(?<=\n(?!((?<![^\"]\"[^\"]).){0," + n + "}\"(\r?\n|,|$))).",
                Pattern.DOTALL);
    }

    // Failed regexes
    // "(?!(([^\"]|\"\")*\"(\r?\n\r?|,|$)))(?:\r?\n\r?).",
    //"(([^\"]{0,1000}(\"\")?){0,100}\"(?:\r?\n|,|$))?[^,\n]*.", // horrible backtracking; times out
    //"\n(?!.{0,50000}(?<!\")\"(\r?\n|,|$))" // somewhat works but too slow!

    public RecordReaderCsv() {
        this(
                RECORD_MINLENGTH_KEY,
                RECORD_MAXLENGTH_KEY,
                RECORD_PROBECOUNT_KEY,
                null);
    }

    public RecordReaderCsv(
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            CustomPattern recordSearchPattern) {
        super(minRecordLengthKey, maxRecordLengthKey, probeRecordCountKey, recordSearchPattern, Accumulating.identity());
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        super.initialize(inputSplit, context);

        Configuration conf = context.getConfiguration();
        long cellMaxLength = conf.getLong(CELL_MAXLENGTH_KEY, CELL_MAXLENGTH_DEFAULT_VALUE);
        this.recordStartPattern = createStartOfCsvRecordPattern(cellMaxLength);

        // Default to EXCEL
        this.requestedCsvFormat = FileInputFormatCsv.getCsvFormat(conf, CSVFormat.EXCEL);

        // The header record is skipped only in postprocessing by createRecordFlow()
        this.effectiveCsvFormat = disableSkipHeaderRecord(requestedCsvFormat);
    }

    protected CSVFormat disableSkipHeaderRecord(CSVFormat csvFormat) {
        return CSVFormat.Builder.create(csvFormat)
                .setSkipHeaderRecord(false)
                .build();
    }

    protected CSVParser newCsvParser(Reader reader) throws IOException {
        return new CSVParser(reader, effectiveCsvFormat);
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

    /**
     * Override createRecordFlow to skip the first record if the
     * requested format demands so.
     * This assumes that the header actually resides on the first split!
     */
    @Override
    protected Flowable<List> createRecordFlow() throws IOException {
        Flowable<List> tmp = super.createRecordFlow();

        if (requestedCsvFormat.getSkipHeaderRecord() && isFirstSplit) {
            tmp = tmp.skip(1);
        }

        return tmp;
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
