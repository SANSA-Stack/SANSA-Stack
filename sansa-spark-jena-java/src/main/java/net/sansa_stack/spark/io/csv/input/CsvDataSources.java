package net.sansa_stack.spark.io.csv.input;

import net.sansa_stack.hadoop.format.univocity.conf.UnivocityHadoopConf;
import net.sansa_stack.hadoop.format.univocity.csv.csv.FileInputFormatCsv;
import net.sansa_stack.hadoop.format.univocity.csv.csv.UnivocityParserFactory;
import net.sansa_stack.hadoop.format.univocity.csv.csv.UnivocityUtils;
import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.model.csvw.domain.impl.CsvwLib;
import org.apache.commons.csv.CSVFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CsvDataSources {

    public static JavaRDD<Binding> createRddOfBindings(
            JavaSparkContext sc,
            String path,
            UnivocityHadoopConf csvConf) throws IOException
    {
        Dialect dialect = csvConf.getDialect();

        UnivocityParserFactory parserFactory = UnivocityParserFactory
                .createDefault(false)
                .configure(csvConf);

        FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

        String[] sampleRow = UnivocityUtils.readCsvRows(path, fs, parserFactory).firstElement().blockingGet();
        List<String> headers;
        // If the headers are not skipped then custom headers are required
        if (!Boolean.FALSE.equals(dialect.getHeader())) {
            headers = Arrays.asList(sampleRow);
        } else {
            int n = sampleRow.length;
            headers = new ArrayList<>(n);
            for (int i = 0; i < n; ++i) {
                headers.add(CsvwLib.getExcelColumnLabel(i));
            }
            // throw new IllegalArgumentException("A custom mapper for bindings must be supplied if there is no header row");
        }

        List<Var> vars = Var.varList(headers);

        return createRddOfBindings(sc, path, csvConf, row -> createBinding(vars, row));
    }

    public static JavaRDD<Binding> createRddOfBindings(
            JavaSparkContext sc,
            String path,
            UnivocityHadoopConf univocityConf,
            Function<List<String>, Binding> mapper)
    {
        Configuration conf = sc.hadoopConfiguration();
        FileInputFormatCsv.setUnivocityConfig(conf, univocityConf);

        JavaRDD<List<String>> rdd;
        if (false) {
            // FileInputFormatCsv.setCsvFormat(conf, csvFormat);
            // Commons-CSV
            rdd = sc.newAPIHadoopFile(path, net.sansa_stack.hadoop.format.commons_csv.csv.FileInputFormatCsv.class, LongWritable.class, List.class, conf)
                    .map(t -> (List<String>) t._2);
        } else {
            // Univocity CSV
            rdd = sc.newAPIHadoopFile(path,
                    net.sansa_stack.hadoop.format.univocity.csv.csv.FileInputFormatCsv.class, LongWritable.class, String[].class, conf)
                    .map(t -> Arrays.asList( t._2));
        }
        JavaRDD<Binding> bindingRdd = rdd.map(mapper);
        return bindingRdd;
    }

    /**
     * Util method to create a binding from a list of variables and a list of strings. The latter will be converted to plain literals.
     * The given lists must have the same length.
     */
    public static Binding createBinding(List<Var> headings, List<String> strs) {
        BindingBuilder builder = BindingBuilder.create();
        Iterator<Var> itVar = headings.iterator();
        Iterator<String> itStr = strs.iterator();

        while (itVar.hasNext()) {
            Var var = itVar.next();
            String str = itStr.hasNext() ? itStr.next() : null;

            if (str != null) {
                builder.add(var, NodeFactory.createLiteral(str));
            }
        }

        Binding result = builder.build();
        return result;
    }

}
