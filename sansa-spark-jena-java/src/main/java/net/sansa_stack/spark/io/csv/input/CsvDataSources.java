package net.sansa_stack.spark.io.csv.input;

import net.sansa_stack.hadoop.format.commons_csv.csv.CsvUtils;
import net.sansa_stack.hadoop.format.commons_csv.csv.FileInputFormatCsv;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CsvDataSources {

    public static JavaRDD<Binding> createRddOfBindings(
            JavaSparkContext sc,
            String path,
            CSVFormat csvFormat) throws IOException
    {
        // If the headers are not skipped then custom headers are required
        if (!csvFormat.getSkipHeaderRecord()) {
            throw new IllegalArgumentException("A custom mapper for bindings must be supplied if there is no header row");
        }

        FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

        List<String> headers = CsvUtils.readCsvRows(path, fs, csvFormat).firstElement().blockingGet();
        List<Var> vars = Var.varList(headers);

        return createRddOfBindings(sc, path, csvFormat, row -> createBinding(vars, row));
    }

    public static JavaRDD<Binding> createRddOfBindings(
            JavaSparkContext sc,
            String path,
            CSVFormat csvFormat,
            Function<List<String>, Binding> mapper)
    {
        Configuration conf = sc.hadoopConfiguration();
        FileInputFormatCsv.setCsvFormat(conf, csvFormat);

        JavaRDD<List<String>> rdd;
        if (false) {
            // Commons-CSV
            rdd = sc.newAPIHadoopFile(path, FileInputFormatCsv.class, LongWritable.class, List.class, conf)
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
