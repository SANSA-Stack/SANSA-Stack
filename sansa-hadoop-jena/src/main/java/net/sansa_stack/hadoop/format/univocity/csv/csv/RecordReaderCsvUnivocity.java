package net.sansa_stack.hadoop.format.univocity.csv.csv;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Streams;
import com.univocity.parsers.common.AbstractParser;
import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.core.Accumulating;
import net.sansa_stack.hadoop.core.RecordReaderGenericBase;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.core.pattern.CustomPatternCsv;
import net.sansa_stack.hadoop.core.pattern.CustomPatternCsvFromCsvw;
import net.sansa_stack.hadoop.core.pattern.CustomPatternJava;
import net.sansa_stack.hadoop.format.jena.base.RecordReaderConf;
import net.sansa_stack.hadoop.format.univocity.conf.UnivocityHadoopConf;
import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;
import org.apache.commons.csv.CSVFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A generic parser implementation for CSV with the offset-seeking condition that
 * CSV rows must all have the same length.
 *
 * Note the csvw dialect "header" attribute is interpreted slightly differently: It controls whether to use
 * the given header names (true) or generate excel header names (false) ("a", "b", ..., "aa", ...)
 * The skip
 *
 */
public class RecordReaderCsvUnivocity
    extends RecordReaderGenericBase<String[], String[], String[], String[]>
{
    // private static final Logger logger = LoggerFactory.getLogger(RecordReaderCsv.class);

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

    /* Factory for parsers (csv/tsv) according to configuration. Initialized during initialize() */
    protected Dialect requestedDialect;
    protected UnivocityParserFactory parserFactory;

    protected long postProcessRowSkipCount;

    /**
     * Create a regex for matching csv record starts.
     * Matches the character following a newline character, whereas that newline is not within a csv cell
     * w.r.t. a certain amount of lookahead.
     *
     * The regex does the following:
     * Match a character (the dot at the end) if the immediate preceeding character is a newline;
     * Match a newline; however only if there is no subsequent sole double quote followed by a newline, comma or eof.
     * Stop matching 'dot' if there was a single quote in the past
     *
     * @param maxCharsPerColumn The maximum number of lookahead bytes to check for whether a newline character
     *          might be within a csv cell
     * @return The corresponding regex pattern
     */
    public static CustomPattern createStartOfCsvRecordPattern(long maxCharsPerColumn) {
        // TODO There must not be an odd number of consecutive quote chars -
        //  the pattern just captures 1 sole char
        return CustomPatternJava.compile(
                "(?<=\n(?!((?<![^\"]\"[^\"]).){0," + maxCharsPerColumn + "}\"(\r?\n|,|$))).",
                Pattern.DOTALL);
    }

    public static Pattern createStartOfCsvRecordPattern(long n, char quoteChar, char quoteEscapeChar, char quoteEscapeEscapeChar) {
        // Pattern for matching a non-escaped quote char
        String quoteCharPattern;
        if (quoteChar == quoteEscapeChar) {
            quoteCharPattern = "[^" + quoteChar + "]" + quoteChar + "[^" + quoteChar + "]";
        } else {
            quoteCharPattern = "[^" + quoteEscapeEscapeChar + "]" + "[^" + quoteEscapeChar + "]" + quoteChar;
        }

        return Pattern.compile(
                "(?<=\n(?!((?<!" + quoteCharPattern + ").){0," + n + "}\"(\r?\n|,|$))).",
                Pattern.DOTALL);
    }


    // Failed regexes
    // "(?!(([^\"]|\"\")*\"(\r?\n\r?|,|$)))(?:\r?\n\r?).",
    //"(([^\"]{0,1000}(\"\")?){0,100}\"(?:\r?\n|,|$))?[^,\n]*.", // horrible backtracking; times out
    //"\n(?!.{0,50000}(?<!\")\"(\r?\n|,|$))" // somewhat works but too slow!

    public RecordReaderCsvUnivocity() {
        this(new RecordReaderConf(
                RECORD_MINLENGTH_KEY,
                RECORD_MAXLENGTH_KEY,
                RECORD_PROBECOUNT_KEY,
                null)); // RecordSearchPattern is set in initialize()
    }

    public RecordReaderCsvUnivocity(RecordReaderConf conf) {
        super(conf, Accumulating.identity());
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        super.initialize(inputSplit, context);

        Configuration conf = context.getConfiguration();

        UnivocityHadoopConf parserConf = FileInputFormatCsvUnivocity.getUnivocityConfig(conf);
        DialectMutable tmp = new DialectMutableImpl();
        parserConf.getDialect().copyInto(tmp);
        requestedDialect = tmp;

        long cellMaxLength = conf.getLong(CELL_MAXLENGTH_KEY, CELL_MAXLENGTH_DEFAULT_VALUE);
        CustomPatternCsv.Config config = CustomPatternCsvFromCsvw.adapt(requestedDialect);
        this.recordStartPattern = CustomPatternCsv.create(config);
        // createStartOfCsvRecordPattern(cellMaxLength);

        // By default assume a header row
        postProcessRowSkipCount = Optional.ofNullable(parserConf.getDialect().getHeaderRowCount()).orElse(1l);


        // Configure the parser to parse all rows
        parserConf.getDialect().setHeaderRowCount(0l);
        this.parserFactory = UnivocityParserFactory.createDefault(false).configure(parserConf);
        // this.effectiveCsvFormat = disableSkipHeaderRecord(requestedTabularFormat);
    }

//    protected CSVFormat disableSkipHeaderRecord(CSVFormat csvFormat) {
//        return CSVFormat.Builder.create(csvFormat)
//                .setSkipHeaderRecord(false)
//                .build();
//    }

    /** State class used for Flowable.generate */
    private static class State
        extends AbstractIterator<String[]>
        implements AutoCloseable
    {
        public Reader reader;
        public Callable<AbstractParser<?>> parserFactory;
        public AbstractParser<?> parser = null;
        public long seenRowLength = -1;
        public String[] priorRow = null;

        State(Callable<AbstractParser<?>> parserFactory, Reader reader) { this.parserFactory = parserFactory; this.reader = reader; }

        @Override
        protected String[] computeNext() {
            String[] result;

            // AbstractParser<?> it = parser;
            if (parser == null) {
                try {
                    parser = parserFactory.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                parser.beginParsing(reader);
            }

            String[] row;
            if ((row = parser.parseNext()) == null) {
                result = endOfData();
            } else {

                long rowLength = row.length;
                if (seenRowLength == -1) {
                    seenRowLength = rowLength;
                } else if (rowLength != seenRowLength) {
                                /*
                                String msg = String.format("Current row length (%d) does not match prior one (%d)", rowLength, s.seenRowLength);
                                logger.error(msg);
                                logger.error("Prior row: " + s.priorRow);
                                logger.error("Current row: " + row);
                                 */
                    String msg = String.format("Current row length (%d) does not match prior one (%d) - current: %s | prior: %s", rowLength, seenRowLength, Arrays.asList(row), Arrays.asList(priorRow));
                    throw new IllegalStateException(msg);
                }
                priorRow = row;
                result = row;
            }

            // if (row != null) { System.err.println("GOT ROW: " + Arrays.asList(row)); }
            return result;
        }

        @Override
        public void close() {
            try {
                if (parser != null) {
                    parser.stopParsing();
                }
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    /**
     * Override createRecordFlow to skip the first record if the
     * requested format demands so.
     * This assumes that the header actually resides on the first split!
     */
    @Override
    protected Stream<String[]> createRecordFlow() throws IOException {
        Stream<String[]> tmp = super.createRecordFlow();

        // Header is true by default
        // if (!Boolean.FALSE.equals(requestedDialect.getHeader()) && isFirstSplit) {
        if (isFirstSplit && postProcessRowSkipCount > 0) {
            tmp = tmp.skip(postProcessRowSkipCount);
            // tmp = tmp.skip(1);
        }

        return tmp;
    }


    protected Stream<String[]> parse(InputStream in, boolean isProbe) {
        State it = new State(parserFactory::newParser, parserFactory.newInputStreamReader(in));
        Stream<String[]> result = Streams.stream(it).onClose(it::close);
        return result;
        // return parse(() -> in).blockingStream();
    }

    // @Override
    protected Flowable<String[]> parse(Callable<InputStream> inputStreamSupplier) {
        return Flowable.generate(
                () -> new State(parserFactory::newParser, parserFactory.newInputStreamReader(inputStreamSupplier.call())),
                (s, e) -> {
//                    BufferedReader br = new BufferedReader(s.reader);
//                    List<String> lines = br.lines().limit(2).collect(Collectors.toList());
//                    System.out.println("Lines: " + lines);
//                    if (true) throw new RuntimeException("dummy exception");
                    try {
                        AbstractParser<?> it = s.parser;
                        if (it == null) {
                            it = s.parser = parserFactory.newParser();
                            s.parser.beginParsing(s.reader);
                        }

                        String[] row;
                        if ((row = it.parseNext()) == null) {
                            e.onComplete();
                        } else {

                            long rowLength = row.length;
                            if (s.seenRowLength == -1) {
                                s.seenRowLength = rowLength;
                            } else if (rowLength != s.seenRowLength) {
                                /*
                                String msg = String.format("Current row length (%d) does not match prior one (%d)", rowLength, s.seenRowLength);
                                logger.error(msg);
                                logger.error("Prior row: " + s.priorRow);
                                logger.error("Current row: " + row);
                                 */
                                String msg = String.format("Current row length (%d) does not match prior one (%d) - current: %s | prior: %s", rowLength, s.seenRowLength, Arrays.asList(row), Arrays.asList(s.priorRow));
                                throw new IllegalStateException(msg);
                            }
                            s.priorRow = row;
                            e.onNext(row);
                        }

                    } catch (Exception x) {
                        e.onError(x);
                    }
                },
                s -> s.reader.close());
    }
}
