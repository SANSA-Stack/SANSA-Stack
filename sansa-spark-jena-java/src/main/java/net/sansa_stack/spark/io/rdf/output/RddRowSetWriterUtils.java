package net.sansa_stack.spark.io.rdf.output;

import net.sansa_stack.hadoop.output.jena.base.OutputFormatRowSet;
import net.sansa_stack.hadoop.output.jena.base.RdfOutputUtils;
import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;
import org.aksw.jenax.io.rowset.core.RowSetStreamWriterRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;

import java.util.List;

/**
 * Static util methods to write {@link JavaResultSetSpark} instances out using hadoop.
 * With {@link #isValidLang(Lang)} lang instances can be checked early for whether they are supported.
 */
public class RddRowSetWriterUtils {
    /** Check if the given lang has a registered hadoop writer */
    public static boolean isValidLang(Lang lang) {
        return RowSetStreamWriterRegistry.isRegistered(lang);
    }

    /** Raises an {@link IllegalArgumentException} if writing out using the given lang is unsupported. */
    public static void requireValidLang(Lang lang) {
        if (!isValidLang(lang)) {
            throw new IllegalArgumentException("Lang " + lang + " is not supported");
        }
    }

    public static void write(JavaResultSetSpark rs, Path path, Lang lang) {
        write(rs.getRdd().rdd(), path, rs.getResultVars(), lang);
    }

    public static void write(RDD<Binding> rdd, Path path, List<Var> vars, Lang lang) {
        requireValidLang(lang);
        Configuration conf = RddWriterUtils.buildBaseConfiguration(rdd);
        RdfOutputUtils.setVars(conf, vars);
        RdfOutputUtils.setLang(conf, lang);

        JavaPairRDD<Long, Binding> pairRdd = RddWriterUtils.toPairRdd(rdd.toJavaRDD());
        pairRdd.saveAsNewAPIHadoopFile(path.toString(),
                Long.class,
                Binding.class,
                OutputFormatRowSet.class,
                conf);
    }
}
