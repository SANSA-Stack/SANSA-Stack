package net.sansa_stack.spark.io.csv.input;

import com.google.gson.JsonObject;
import org.aksw.commons.lambda.serializable.SerializableBiFunction;
import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.aksw.jena_sparql_api.sparql.ext.json.RDFDatatypeJson;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CsvRowMapperFactories {

    public static Function<String[], Binding> rowMapperFactoryBinding(String[][] header) {
        Var[][] vars = headerToVars(header);
        SerializableFunction<String[], Binding> result = strs -> rowToBinding(vars, strs);
        return result;
    }

    /** A RowMapperFactory that uses a predefined set of variables */
    public static Function<String[][], Function<String[], Binding>> rowMapperFactoryBinding(Var[] header) {
        SerializableFunction<String[][], Function<String[], Binding>> result = columnNames -> {
            SerializableFunction<String[], Binding> r = strs -> rowToBinding(header, strs);
            return r;
        };
        return result;
    }

    /**
     * Wrap a rowMapperFactoryJson such that it produces bindings
     */
    public static Function<String[][], Function<String[], Binding>> rowMapperFactoryJson(Binding parent, Var resultVar, SerializableBiFunction<String[][], String[], JsonObject> rowMapperFactoryJson) {
        SerializableFunction<String[][], Function<String[], Binding>> result = headers -> {
            SerializableFunction<String[], Binding> r = strs -> {
                JsonObject obj = rowMapperFactoryJson.apply(headers, strs);
                Node node = RDFDatatypeJson.get().jsonToNode(obj);
                BindingBuilder bb = BindingBuilder.create(parent);
                bb.add(resultVar, node);
                return bb.build();
            };
            return r;
        };
        return result;
    }

//    public static SerializableFunction<String[], Binding> rowMapperFactoryJson(String[][] headings) {
//        return strs -> rowToJson(headings, strs);
//    }

    /*
    public static <T> JavaRDD<T> createRdd(
            JavaSparkContext sc,
            String path,
            UnivocityCsvwConf univocityConf,
            Function<String[], T> mapper)
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
        JavaRDD<T> bindingRdd = rdd.map(mapper);
        return bindingRdd;
    }
*/

    public static String[][] transformHeader(String[][] input, Function<String, String> transform) {
        return Arrays.stream(input)
                .map(arr -> Arrays.stream(arr)
                        .map(transform)
                        .collect(Collectors.toList()).toArray(new String[0]))
                .collect(Collectors.toList()).toArray(new String[0][]);
    }

    public static Var[][] headerToVars(String[][] columnNames) {
        int n = columnNames.length;
        Var[][] result = new Var[n][];
        for (int i = 0; i < n; ++i) {
            String[] strs = columnNames[i];
            Var[] h = new Var[strs.length];
            result[i] = h;
            for (int j = 0; j < strs.length; ++j) {
                String str = strs[j];
                h[j] = Var.alloc(str);
            }
        }
        return result;
    }

    /**
     * Util method to create a binding from a list of variables and a list of strings.
     * The latter will be converted to plain literals.
     * The given lists must have the same length.
     * <p>
     * A single column may have zero or more headers.
     * This allows for the same value to be exposed under multiple variables
     * in the returned binding.
     */
    public static Binding rowToBinding(Var[][] headers, String[] strs) {
        BindingBuilder builder = BindingBuilder.create();
        int n = Math.min(headers.length, strs.length);
        for (int i = 0; i < n; ++i) {
            Var[] vars = headers[i];
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

    public static Binding rowToBinding(Var[] headers, String[] strs) {
        BindingBuilder builder = BindingBuilder.create();
        int n = Math.min(headers.length, strs.length);
        for (int i = 0; i < n; ++i) {
            Var var = headers[i];
            String str = strs[i];
            if (str != null) {
                Node node = NodeFactory.createLiteral(str);
                builder.add(var, node);
            }
        }
        Binding result = builder.build();
        return result;
    }
    public static JsonObject rowToJson(String[][] headers, String[] strs) {
        JsonObject result = new JsonObject();
        int n = Math.min(headers.length, strs.length);
        for (int i = 0; i < n; ++i) {
            String[] vars = headers[i];
            String str = strs[i];
            if (str != null) {
                int varlen = vars.length;
                for (int j = 0; j < varlen; ++j) {
                    String var = vars[j];
                    result.addProperty(var, str);
                }
            }
        }
        return result;
    }

}
