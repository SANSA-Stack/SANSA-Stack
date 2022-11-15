package net.sansa_stack.spark.io.csv.input;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;
import org.aksw.commons.model.csvw.univocity.CsvwUnivocityUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.StandardSystemProperty;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import net.sansa_stack.hadoop.format.univocity.conf.UnivocityHadoopConf;
import net.sansa_stack.hadoop.format.univocity.csv.csv.FileInputFormatCsvUnivocity;
import net.sansa_stack.hadoop.format.univocity.csv.csv.UnivocityParserFactory;
import net.sansa_stack.hadoop.format.univocity.csv.csv.UnivocityUtils;
import net.sansa_stack.hadoop.util.FileSystemUtils;
import net.sansa_stack.spark.io.rdf.input.api.HadoopInputData;
import net.sansa_stack.spark.io.rdf.input.api.InputFormatUtils;

public class CsvDataSources {
    private static final Logger logger = LoggerFactory.getLogger(CsvDataSources.class);

    public static JavaRDD<Binding> createRddOfBindings(
            JavaSparkContext sc,
            String pathStr,
            UnivocityHadoopConf csvConf) throws IOException
    {
        return createRddOfBindings(sc, pathStr, csvConf, Arrays.asList("row"));
    }

    public static JavaRDD<Binding> createRddOfBindings(
            JavaSparkContext sc,
            String pathStr,
            UnivocityHadoopConf baseCsvConf,
            List<String> columnNamingSchemes
            ) throws IOException
    {
        Configuration conf = new Configuration(sc.hadoopConfiguration());
        HadoopInputData<?, String[], JavaRDD<Binding>> hid = configureHadoop(conf, pathStr, baseCsvConf, columnNamingSchemes);
        JavaRDD<Binding> result = InputFormatUtils.createRdd(sc, hid);
        return result;
    }

    public static HadoopInputData<LongWritable, String[], JavaRDD<Binding>> configureHadoop(
            Configuration conf,
            String pathStr,
            UnivocityHadoopConf baseCsvConf,
            List<String> columnNamingSchemes) throws IOException {
        Path path = new Path(pathStr);
        Callable<InputStream> inputStreamFactory = () -> FileSystemUtils.newInputStream(path, conf);

        Dialect dialect = baseCsvConf.getDialect();
        Long headerRowCountBak = dialect.getHeaderRowCount();
        boolean hasHeaders = !Long.valueOf(0).equals(headerRowCountBak);

        DialectMutable effectiveDialect = new DialectMutableImpl();
        dialect.copyInto(effectiveDialect, false);

        // Don't skip any rows while we sample the first row
        effectiveDialect.setHeaderRowCount(0l);

        UnivocityHadoopConf csvConf = new UnivocityHadoopConf(effectiveDialect);
        UnivocityParserFactory parserFactory = UnivocityParserFactory
                .createDefault(false)
                .configure(csvConf);

        // Auto detect any missing CSV settings
        if (!baseCsvConf.isTabs()) {
            CsvParserSettings csvSettings = parserFactory.getCsvSettings();

            Set<String> detectedProperties;
            try {
                UnivocityParserFactory finalParserFactory = parserFactory;
                detectedProperties = CsvwUnivocityUtils.configureDialect(
                        effectiveDialect, csvSettings,
                        () -> (CsvParser)finalParserFactory.newParser(),
                        () -> finalParserFactory.newInputStreamReader(inputStreamFactory.call()));
            } catch (Exception e) {
                throw new IOException();
            }

            logger.info("For source " + pathStr + " auto-detected csv properties: " + detectedProperties);

            csvConf.setTabs(false);
            parserFactory = UnivocityParserFactory
                    .createDefault(false)
                    .configure(csvConf);
        }

        // TODO When we probe for the CSV dialect above we could actually
        // reuse that parser to also extract the headers
        // Right now we start another parser for the headers

        // String[] sampleRow = UnivocityRxUtils.readCsvRows(path, fs, parserFactory).firstElement().blockingGet();
        String[] sampleRow;
        try (Stream<String[]> rows = UnivocityUtils.readCsvRows(inputStreamFactory, parserFactory)) {
            sampleRow = rows.findFirst().orElse(new String[0]);
        }

        // Pass on the configuration with the original header row config
        effectiveDialect.setHeaderRowCount(headerRowCountBak);

        int n = sampleRow.length;

        String[][] columnNames = ColumnNamingScheme.createColumnHeadings(columnNamingSchemes, sampleRow, !hasHeaders);
        Var[][] headers = new Var[n][];
        for (int i = 0; i < n; ++i) {
            String[] strs = columnNames[i];
            Var[] h = new Var[strs.length];
            headers[i] = h;
            for (int j = 0; j < strs.length; ++j) {
                h[j] = Var.alloc(strs[j]);
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info(String.format("Effective CSV dialect for %s:%s%s", pathStr, StandardSystemProperty.LINE_SEPARATOR.value(), effectiveDialect));
        }

        // Configuration conf = sc.hadoopConfiguration();
        FileInputFormatCsvUnivocity.setUnivocityConfig(conf, csvConf);
        return new HadoopInputData<>(pathStr, FileInputFormatCsvUnivocity.class, LongWritable.class, String[].class, conf,
                javaPairRdd -> javaPairRdd.map(row -> createBinding(headers, row._2)));
    }

    public static JavaRDD<Binding> createRddOfBindings(
            JavaSparkContext sc,
            String path,
            UnivocityHadoopConf univocityConf,
            Function<String[], Binding> mapper)
    {
        Configuration conf = sc.hadoopConfiguration();
        FileInputFormatCsvUnivocity.setUnivocityConfig(conf, univocityConf);

        JavaRDD<String[]> rdd;
        if (false) {
            // FileInputFormatCsv.setCsvFormat(conf, csvFormat);
            // Commons-CSV
            rdd = sc.newAPIHadoopFile(path, net.sansa_stack.hadoop.format.commons_csv.csv.FileInputFormatCsv.class, LongWritable.class, List.class, conf)
                    .map(t -> (List<String>)t._2)
                    .map(t -> t.toArray(new String[0]));
        } else {
            // Univocity CSV
            rdd = sc.newAPIHadoopFile(path,
                    FileInputFormatCsvUnivocity.class, LongWritable.class, String[].class, conf)
                    // .map(t -> Arrays.asList(t._2));
                    .map(t -> t._2);
        }
        JavaRDD<Binding> bindingRdd = rdd.map(mapper);
        return bindingRdd;
    }

    /**
     * Util method to create a binding from a list of variables and a list of strings.
     * The latter will be converted to plain literals.
     * The given lists must have the same length.
     *
     * A single column may have zero or more headings.
     * This allows for the same value to be exposed under multiple variables
     * in the returned binding.
     */
    public static Binding createBinding(Var[][] headings, String[] strs) {
        BindingBuilder builder = BindingBuilder.create();
        int n = Math.min(headings.length, strs.length);
        for (int i = 0; i < n; ++i) {
            Var[] vars = headings[i];
            String str = strs[i];
            if (str != null) {
                int varlen = vars.length;
                Node node = NodeFactory.createLiteral(str);
                for (int j = 0; j < varlen; ++j) {
                    Var var = vars[j];
                    builder.add(var, node);
                }
            }
        }
        Binding result = builder.build();
        return result;
    }

}


//Path path = new Path(pathStr);
//Callable<InputStream> inputStreamFactory = () -> FileSystemUtils.newInputStream(path, sc.hadoopConfiguration());
//
//Dialect dialect = baseCsvConf.getDialect();
//Long headerRowCountBak = dialect.getHeaderRowCount();
//boolean hasHeaders = !Long.valueOf(0).equals(headerRowCountBak);
//
//DialectMutable effectiveDialect = new DialectMutableImpl();
//dialect.copyInto(effectiveDialect);
//
//// Don't skip any rows while we sample the first row
//effectiveDialect.setHeaderRowCount(0l);
//
//UnivocityHadoopConf csvConf = new UnivocityHadoopConf(effectiveDialect);
//UnivocityParserFactory parserFactory = UnivocityParserFactory
//      .createDefault(false)
//      .configure(csvConf);
//
//// Auto detect any missing CSV settings
//if (!baseCsvConf.isTabs()) {
//  CsvParserSettings csvSettings = parserFactory.getCsvSettings();
//
//  Set<String> detectedProperties;
//  try {
//      UnivocityParserFactory finalParserFactory = parserFactory;
//      detectedProperties = CsvwUnivocityUtils.configureDialect(
//              effectiveDialect, csvSettings,
//              () -> (CsvParser)finalParserFactory.newParser(),
//              () -> finalParserFactory.newInputStreamReader(inputStreamFactory.call()));
//  } catch (Exception e) {
//      throw new IOException();
//  }
//
//  logger.info("For source " + pathStr + " auto-detected csv properties: " + detectedProperties);
//
//  csvConf.setTabs(false);
//  parserFactory = UnivocityParserFactory
//          .createDefault(false)
//          .configure(csvConf);
//}
//
//// TODO When we probe for the CSV dialect above we could actually
//// reuse that parser to also extract the headers
//// Right now we start another parser for the headers
//
//// String[] sampleRow = UnivocityRxUtils.readCsvRows(path, fs, parserFactory).firstElement().blockingGet();
//String[] sampleRow;
//try (Stream<String[]> rows = UnivocityUtils.readCsvRows(inputStreamFactory, parserFactory)) {
//  sampleRow = rows.findFirst().orElse(new String[0]);
//}
//
//// Pass on the configuration with the original header row config
//effectiveDialect.setHeaderRowCount(headerRowCountBak);
//
//int n = sampleRow.length;
//
//String[][] columnNames = ColumnNamingScheme.createColumnHeadings(columnNamingSchemes, sampleRow, !hasHeaders);
//Var[][] headers = new Var[n][];
//for (int i = 0; i < n; ++i) {
//  String[] strs = columnNames[i];
//  Var[] h = new Var[strs.length];
//  headers[i] = h;
//  for (int j = 0; j < strs.length; ++j) {
//      h[j] = Var.alloc(strs[j]);
//  }
//}
//
//if (logger.isInfoEnabled()) {
//  logger.info(String.format("Effective CSV dialect for %s:%s%s", pathStr, StandardSystemProperty.LINE_SEPARATOR.value(), effectiveDialect));
//}
//
//return createRddOfBindings(sc, pathStr, csvConf, row -> createBinding(headers, row));
